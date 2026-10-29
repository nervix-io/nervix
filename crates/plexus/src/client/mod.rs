//! Connector-specific client definitions in Plexus.
//!
//! Layer: language.
//! - **Owns.** Composed connector/client heads and their typed configuration, mount and pool clauses.
//! - **Depends on.** Shared language tokens, parser primitives and client vocabulary Models.
//! - **Must not know.** Registry validation, connector drivers or runtime execution.

#![cfg_attr(
    nervix_lint,
    nervix::layer(language, reason = "owns the client definition grammar declared above")
)]

use chumsky::prelude::*;
use meticulous::OptionExt as _;
use nervix_models::{
    ClientConfigEntry, ClientConfigSchema, ClientName, ClientPoolBounds, ClientResourceMount,
    CreateClientAzureBlob, CreateClientClickHouse, CreateClientGcs, CreateClientHttp,
    CreateClientIcebergRest, CreateClientKafka, CreateClientMongoDb, CreateClientMqtt,
    CreateClientMySql, CreateClientNats, CreateClientOtel, CreateClientPostgres,
    CreateClientPrometheus, CreateClientPulsar, CreateClientRabbitMq, CreateClientRedis,
    CreateClientS3, CreateClientSentry, CreateClientSqs, CreateClientSyslog,
    CreateClientWebsockets, CreateClientZeroMq, CreateStatement, Model, RequestedResourceVersion,
};

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        LexedInput, ParseError, boxed_choice, client_name, if_not_exists_clause, into_parse_error,
        kw, kw_phrase2, lex_input, nonzero_u32_value, resource_ref, resource_version_clause,
        signaling_protocol_clause, string_lit, suggest_from, tok, u32_value, word_raw,
    },
    schema::ParseFromSourceError,
};

fn scalar_value<'src>()
-> impl Parser<'src, &'src [Token], String, extra::Err<ParseError<'src>>> + Clone {
    choice((
        string_lit(),
        select! { Token::NumberLiteral(v) => v },
        word_raw(),
    ))
}

fn config_key<'src>(
    schema: ClientConfigSchema,
) -> impl Parser<'src, &'src [Token], String, extra::Err<ParseError<'src>>> + Clone {
    let mut alternatives = Vec::new();
    for key in schema.keys() {
        let name = key.name.to_string();
        alternatives.push(just(Token::StringLiteral(name.clone())).to(name).boxed());
    }
    // The registry owns key acceptance, including delegated driver namespaces. Keeping the
    // general literal in this composed grammar lets it diagnose an unknown key at CREATE.
    alternatives.push(string_lit().labelled("config_key").boxed());
    choice(alternatives)
}

fn config_value<'src>()
-> impl Parser<'src, &'src [Token], String, extra::Err<ParseError<'src>>> + Clone {
    scalar_value().labelled("config_value")
}

fn config_entry<'src>(
    schema: ClientConfigSchema,
) -> impl Parser<'src, &'src [Token], ClientConfigEntry, extra::Err<ParseError<'src>>> + Clone {
    config_key(schema)
        .then_ignore(tok(Token::Eq))
        .then(config_value())
        .map(|(key, value)| ClientConfigEntry { key, value })
}

fn client_mount<'src>() -> impl Parser<
    'src,
    &'src [Token],
    Option<ClientResourceMount<RequestedResourceVersion>>,
    extra::Err<ParseError<'src>>,
> + Clone {
    kw(Identifier::Mount)
        .ignore_then(resource_ref().labelled("resource_name"))
        .then(resource_version_clause())
        .map(|(resource, version)| ClientResourceMount { resource, version })
        .or_not()
}

/// The `POOL SIZE MIN <n> MAX <n>` bounds every pool-capable client declares.
///
/// `POOL SIZE` opens the clause and its two bounds follow as one unit, so the ordering check has
/// both counts and a single span to blame, and a client type that has no pool cannot pick up half
/// of the contract.
fn client_pool_bounds<'src>()
-> impl Parser<'src, &'src [Token], ClientPoolBounds, extra::Err<ParseError<'src>>> + Clone {
    kw_phrase2(Identifier::Pool, Identifier::Size)
        .ignore_then(kw(Identifier::Min))
        .ignore_then(u32_value("min_pool_size"))
        .then_ignore(kw(Identifier::Max))
        .then(nonzero_u32_value(
            "max_pool_size",
            "maximum pool size must be greater than zero",
        ))
        .try_map(|(minimum, maximum), span| {
            ClientPoolBounds::new(minimum, maximum)
                .map_err(|error| Rich::custom(span, error.to_string()))
        })
        .boxed()
}

/// The shape every connector-headed client definition shares.
///
/// `before_mount` is the one part that varies: the two declared bounds of a pool-capable
/// client, the optional signaling protocol of a WebSockets client, and nothing at all for the rest.
/// Keeping one chain is what makes `IF NOT EXISTS`, the name, the mount, the configuration block
/// and the optional terminator identical for every client type.
fn create_client_parser<'src, B: 'src, T>(
    connector: Identifier,
    schema: ClientConfigSchema,
    before_mount: impl Parser<'src, &'src [Token], B, extra::Err<ParseError<'src>>> + Clone + 'src,
    build: impl Fn(
        ClientName,
        B,
        Option<ClientResourceMount<RequestedResourceVersion>>,
        Vec<ClientConfigEntry>,
    ) -> T
    + Clone
    + 'src,
) -> impl Parser<'src, &'src [Token], CreateStatement<T>, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Create)
        .ignore_then(if_not_exists_clause())
        .then_ignore(kw_phrase2(connector, Identifier::Client))
        .then(client_name())
        .then(before_mount)
        .then(client_mount())
        .then_ignore(kw(Identifier::Config))
        .then(transport_config(schema))
        .then_ignore(tok(Token::Semicolon).or_not())
        .map(move |((((if_not_exists, name), between), mount), config)| {
            CreateStatement::new(build(name, between, mount, config), if_not_exists)
        })
}

macro_rules! declare_client_parsers {
    (
        before_websockets {
            $($before_parser:ident: $BeforeKeyword:ident => $BeforeClient:ident($BeforeVariant:ident),)+
        }
        after_websockets {
            $($after_parser:ident: $AfterKeyword:ident => $AfterClient:ident($AfterVariant:ident),)+
        }
        pooled {
            $($pooled_parser:ident: $PooledKeyword:ident => $PooledClient:ident($PooledVariant:ident),)+
        }
    ) => {
        $(
            pub fn $before_parser<'src>() -> impl Parser<
                'src,
                &'src [Token],
                CreateStatement<$BeforeClient<RequestedResourceVersion>>,
                extra::Err<ParseError<'src>>,
            > + Clone {
                create_client_parser(
                    Identifier::$BeforeKeyword,
                    <$BeforeClient<RequestedResourceVersion>>::DECLARATION.config,
                    empty(),
                    |name, (), mount, config| $BeforeClient {
                        name,
                        mount,
                        config,
                    },
                )
            }
        )+
        $(
            pub fn $after_parser<'src>() -> impl Parser<
                'src,
                &'src [Token],
                CreateStatement<$AfterClient<RequestedResourceVersion>>,
                extra::Err<ParseError<'src>>,
            > + Clone {
                create_client_parser(
                    Identifier::$AfterKeyword,
                    <$AfterClient<RequestedResourceVersion>>::DECLARATION.config,
                    empty(),
                    |name, (), mount, config| $AfterClient {
                        name,
                        mount,
                        config,
                    },
                )
            }
        )+
        $(
            pub fn $pooled_parser<'src>() -> impl Parser<
                'src,
                &'src [Token],
                CreateStatement<$PooledClient<RequestedResourceVersion>>,
                extra::Err<ParseError<'src>>,
            > + Clone {
                create_client_parser(
                    Identifier::$PooledKeyword,
                    <$PooledClient<RequestedResourceVersion>>::DECLARATION.config,
                    client_pool_bounds(),
                    |name, pool, mount, config| $PooledClient {
                        name,
                        pool,
                        mount,
                        config,
                    },
                )
            }
        )+

        pub fn create_client_model_parser<'src>() -> impl Parser<
            'src,
            &'src [Token],
            CreateStatement<Box<Model<RequestedResourceVersion>>>,
            extra::Err<ParseError<'src>>,
        > + Clone {
            boxed_choice!(
                $(
                    $before_parser().map(|create| {
                        create.map_body(Model::$BeforeVariant).map_body(Box::new)
                    }),
                )+
                create_client_websockets_parser().map(|create| {
                    create
                        .map_body(Model::ClientWebsockets)
                        .map_body(Box::new)
                }),
                $(
                    $after_parser().map(|create| {
                        create.map_body(Model::$AfterVariant).map_body(Box::new)
                    }),
                )+
                $(
                    $pooled_parser().map(|create| {
                        create.map_body(Model::$PooledVariant).map_body(Box::new)
                    }),
                )+
            )
        }
    };
}

declare_client_parsers! {
    before_websockets {
        create_client_kafka_parser: Kafka => CreateClientKafka(ClientKafka),
        create_client_pulsar_parser: Pulsar => CreateClientPulsar(ClientPulsar),
        create_client_http_parser: Http => CreateClientHttp(ClientHttp),
        create_client_sentry_parser: Sentry => CreateClientSentry(ClientSentry),
        create_client_otel_parser: Otel => CreateClientOtel(ClientOtel),
        create_client_prometheus_parser: Prometheus => CreateClientPrometheus(ClientPrometheus),
        create_client_rabbitmq_parser: Rabbitmq => CreateClientRabbitMq(ClientRabbitMq),
        create_client_mqtt_parser: Mqtt => CreateClientMqtt(ClientMqtt),
        create_client_nats_parser: Nats => CreateClientNats(ClientNats),
        create_client_zeromq_parser: Zeromq => CreateClientZeroMq(ClientZeroMq),
        create_client_sqs_parser: Sqs => CreateClientSqs(ClientSqs),
        create_client_s3_parser: S3 => CreateClientS3(ClientS3),
        create_client_gcs_parser: Gcs => CreateClientGcs(ClientGcs),
        create_client_azure_blob_parser: AzureBlob => CreateClientAzureBlob(ClientAzureBlob),
        create_client_iceberg_rest_parser: IcebergRest => CreateClientIcebergRest(ClientIcebergRest),
    }
    after_websockets {
        create_client_syslog_parser: Syslog => CreateClientSyslog(ClientSyslog),
        create_client_clickhouse_parser: Clickhouse => CreateClientClickHouse(ClientClickHouse),
    }
    pooled {
        create_client_redis_parser: Redis => CreateClientRedis(ClientRedis),
        create_client_postgres_parser: Postgres => CreateClientPostgres(ClientPostgres),
        create_client_mysql_parser: Mysql => CreateClientMySql(ClientMySql),
        create_client_mongodb_parser: Mongodb => CreateClientMongoDb(ClientMongoDb),
    }
}

pub fn create_client_websockets_parser<'src>() -> impl Parser<
    'src,
    &'src [Token],
    CreateStatement<CreateClientWebsockets<RequestedResourceVersion>>,
    extra::Err<ParseError<'src>>,
> + Clone {
    create_client_parser(
        Identifier::Websockets,
        <CreateClientWebsockets<RequestedResourceVersion>>::DECLARATION.config,
        signaling_protocol_clause().or_not(),
        |name, signaling_protocol, mount, config| CreateClientWebsockets {
            name,
            mount,
            signaling_protocol,
            config,
        },
    )
}

fn transport_config<'src>(
    schema: ClientConfigSchema,
) -> impl Parser<'src, &'src [Token], Vec<ClientConfigEntry>, extra::Err<ParseError<'src>>> + Clone
{
    config_entry(schema)
        .separated_by(tok(Token::Comma))
        .allow_trailing()
        .collect::<Vec<_>>()
        .delimited_by(tok(Token::LBrace), tok(Token::RBrace))
}

pub fn parse_create_client_kafka_tokens(
    tokens: &[Token],
) -> Result<CreateStatement<CreateClientKafka<RequestedResourceVersion>>, Vec<ParseError<'_>>> {
    let out = create_client_kafka_parser()
        .then_ignore(end())
        .parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .verified("has_errors returned false above, so this parse produced output"))
    }
}

pub fn parse_create_client_kafka(
    input: &str,
) -> error_stack::Result<
    CreateStatement<CreateClientKafka<RequestedResourceVersion>>,
    ParseFromSourceError,
> {
    let LexedInput {
        source,
        spanned_tokens,
        tokens,
    } = lex_input(input)?;
    parse_create_client_kafka_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_create_client_kafka(input: &str, cursor: usize) -> Vec<String> {
    suggest_from!(input, cursor, create_client_kafka_parser())
}

#[cfg(test)]
mod conformance_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lexer::lex;

    fn to_tokens(input: &str) -> Vec<Token> {
        lex(input)
            .expect("lexer should succeed")
            .into_iter()
            .map(|t| t.token)
            .collect()
    }

    #[test]
    fn parses_client_kafka_config() {
        let input = r#"
            CREATE KAFKA CLIENT kafka_main
              CONFIG {
                'bootstrap.servers' = 'host1:9092,host2:9092',
                'group.id' = 'my-consumer-group',
                'auto.offset.reset' = 'earliest',
                'enable.auto.commit' = true
              };
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_client_kafka_tokens(&tokens).expect("parse should succeed");

        assert_eq!(parsed.name.as_str(), "kafka_main");
        assert_eq!(parsed.config.len(), 4);
        assert_eq!(parsed.config[0].key, "bootstrap.servers");
        assert_eq!(parsed.config[0].value, "host1:9092,host2:9092");
        assert_eq!(parsed.config[3].key, "enable.auto.commit");
        assert_eq!(parsed.config[3].value, "true");
    }

    #[test]
    fn parses_client_kafka_mount_clause() {
        let input = r#"
            CREATE KAFKA CLIENT kafka_tls
              MOUNT dev_tls VERSION 4
              CONFIG {
                'ssl.ca.location' = '{{dev_tls}}/ca.pem'
              };
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_client_kafka_tokens(&tokens).expect("parse should succeed");

        assert_eq!(parsed.name.as_str(), "kafka_tls");
        assert_eq!(
            parsed.mount.as_ref().map(|mount| mount.resource.as_str()),
            Some("dev_tls")
        );
        assert_eq!(
            parsed.mount.as_ref().map(|mount| mount.version),
            Some(RequestedResourceVersion::Number(4))
        );
        assert_eq!(parsed.config[0].value, "{{dev_tls}}/ca.pem");
    }

    #[test]
    fn parses_syslog_client_with_resource_mount() {
        let tokens = to_tokens(
            "CREATE SYSLOG CLIENT syslog_tls MOUNT tls_bundle VERSION LATEST CONFIG { 'protocol' \
             = 'tls', 'addr' = 'logs.example.com:6514', 'tls_ca_file' = '{{tls_bundle}}/ca.pem' };",
        );
        let parsed = create_client_syslog_parser()
            .then_ignore(end())
            .parse(tokens.as_slice())
            .into_result()
            .expect("parse should succeed");

        assert_eq!(parsed.name.as_str(), "syslog_tls");
        assert_eq!(
            parsed.mount.as_ref().map(|mount| mount.resource.as_str()),
            Some("tls_bundle")
        );
        assert_eq!(
            parsed.mount.as_ref().map(|mount| mount.version),
            Some(RequestedResourceVersion::Latest)
        );
        assert_eq!(parsed.config.len(), 3);
    }

    #[test]
    fn parses_client_gcs_config() {
        let input = r#"
            CREATE GCS CLIENT gcs_main
              CONFIG {
                'service_path' = 'http://127.0.0.1:4443',
                'no_auth' = true
              };
        "#;

        let tokens = to_tokens(input);
        let parsed = create_client_gcs_parser()
            .then_ignore(end())
            .parse(tokens.as_slice())
            .into_result()
            .expect("parse should succeed");

        assert_eq!(parsed.name.as_str(), "gcs_main");
        assert_eq!(parsed.config.len(), 2);
        assert_eq!(parsed.config[0].key, "service_path");
        assert_eq!(parsed.config[0].value, "http://127.0.0.1:4443");
        assert_eq!(parsed.config[1].key, "no_auth");
        assert_eq!(parsed.config[1].value, "true");
    }

    #[test]
    fn parses_client_azure_blob_config() {
        let input = r#"
            CREATE AZURE_BLOB CLIENT azure_main
              CONFIG {
                'account_name' = 'devstoreaccount1',
                'account_key' = 'local-key'
              };
        "#;

        let tokens = to_tokens(input);
        let parsed = create_client_azure_blob_parser()
            .then_ignore(end())
            .parse(tokens.as_slice())
            .into_result()
            .expect("parse should succeed");

        assert_eq!(parsed.name.as_str(), "azure_main");
        assert_eq!(parsed.config.len(), 2);
        assert_eq!(parsed.config[0].key, "account_name");
        assert_eq!(parsed.config[0].value, "devstoreaccount1");
        assert_eq!(parsed.config[1].key, "account_key");
        assert_eq!(parsed.config[1].value, "local-key");
    }

    #[test]
    fn parses_client_iceberg_rest_config() {
        let input = r#"
            CREATE ICEBERG_REST CLIENT iceberg_catalog
              CONFIG {
                'uri' = 'http://127.0.0.1:8181',
                'warehouse' = 's3://nervix-iceberg/warehouse'
              };
        "#;

        let tokens = to_tokens(input);
        let parsed = create_client_iceberg_rest_parser()
            .then_ignore(end())
            .parse(tokens.as_slice())
            .into_result()
            .expect("parse should succeed");

        assert_eq!(parsed.name.as_str(), "iceberg_catalog");
        assert_eq!(parsed.config.len(), 2);
        assert_eq!(parsed.config[0].key, "uri");
        assert_eq!(parsed.config[0].value, "http://127.0.0.1:8181");
        assert_eq!(parsed.config[1].key, "warehouse");
        assert_eq!(parsed.config[1].value, "s3://nervix-iceberg/warehouse");
    }

    #[test]
    fn parses_client_pulsar_config() {
        let input = r#"
            CREATE PULSAR CLIENT pulsar_main
              CONFIG {
                'addr' = 'pulsar://127.0.0.1:6650',
                'namespace' = 'public/default'
              };
        "#;

        let tokens = to_tokens(input);
        let parsed = create_client_pulsar_parser()
            .then_ignore(end())
            .parse(tokens.as_slice())
            .into_result()
            .expect("parse should succeed");

        assert_eq!(parsed.name.as_str(), "pulsar_main");
        assert_eq!(parsed.config.len(), 2);
        assert_eq!(parsed.config[0].key, "addr");
        assert_eq!(parsed.config[0].value, "pulsar://127.0.0.1:6650");
        assert_eq!(parsed.config[1].key, "namespace");
    }

    #[test]
    fn fails_without_config_block() {
        let tokens = to_tokens("CREATE KAFKA CLIENT kafka_main CONFIG;");
        let errs = parse_create_client_kafka_tokens(&tokens).expect_err("must fail");
        assert!(!errs.is_empty());
    }

    #[test]
    fn fails_when_mount_has_no_resource_name() {
        let tokens = to_tokens("CREATE KAFKA CLIENT kafka_main MOUNT CONFIG { 'a' = 'b' };");
        let errs = parse_create_client_kafka_tokens(&tokens).expect_err("must fail");
        assert!(!errs.is_empty());
    }

    #[test]
    fn mount_context_suggests_config_without_type_leakage() {
        let input = "CREATE KAFKA CLIENT kafka_main MOUNT dev_tls VERSION 1 ";
        let suggestions = suggest_create_client_kafka(input, input.len());
        assert!(suggestions.contains(&"CONFIG".to_string()));
        assert!(!suggestions.contains(&"HTTP".to_string()));
        assert!(!suggestions.contains(&"RABBITMQ".to_string()));
    }

    #[test]
    fn mount_requires_a_resource_version() {
        let input = "CREATE KAFKA CLIENT kafka_main MOUNT dev_tls CONFIG { 'a' = 'b' };";
        assert!(parse_create_client_kafka(input).is_err());
    }

    #[test]
    fn mount_context_requires_version_then_suggests_available_versions() {
        let after_resource = "CREATE KAFKA CLIENT kafka_main MOUNT dev_tls ";
        let suggestions = suggest_create_client_kafka(after_resource, after_resource.len());
        assert_eq!(suggestions, vec!["VERSION".to_string()]);

        let after_version = "CREATE KAFKA CLIENT kafka_main MOUNT dev_tls VERSION ";
        let suggestions = suggest_create_client_kafka(after_version, after_version.len());
        assert!(suggestions.contains(&"LATEST".to_string()));
        assert!(suggestions.contains(&"completed_resource_version".to_string()));
        assert!(!suggestions.contains(&"CONFIG".to_string()));
    }

    #[test]
    fn parses_client_rabbitmq_config() {
        let input = r#"
            CREATE RABBITMQ CLIENT rabbit_main
              CONFIG {
                'addr' = 'amqp://guest:guest@localhost:5672/%2f',
                'connection_name' = 'nervix-rabbit'
              };
        "#;

        let tokens = to_tokens(input);
        let parsed = create_client_rabbitmq_parser()
            .then_ignore(end())
            .parse(tokens.as_slice())
            .into_result()
            .expect("parse should succeed");

        assert_eq!(parsed.name.as_str(), "rabbit_main");
        assert_eq!(parsed.config.len(), 2);
        assert_eq!(parsed.config[0].key, "addr");
        assert_eq!(
            parsed.config[0].value,
            "amqp://guest:guest@localhost:5672/%2f"
        );
        assert_eq!(parsed.config[1].key, "connection_name");
    }

    #[test]
    fn parses_client_http_config() {
        let input = r#"
            CREATE HTTP CLIENT http_main
              CONFIG {
                'endpoint' = 'https://api.example.com/events',
                'timeout_ms' = 5000
              };
        "#;

        let tokens = to_tokens(input);
        let parsed = create_client_http_parser()
            .then_ignore(end())
            .parse(tokens.as_slice())
            .into_result()
            .expect("parse should succeed");

        assert_eq!(parsed.name.as_str(), "http_main");
        assert_eq!(parsed.config.len(), 2);
        assert_eq!(parsed.config[0].key, "endpoint");
        assert_eq!(parsed.config[0].value, "https://api.example.com/events");
        assert_eq!(parsed.config[1].key, "timeout_ms");
    }

    #[test]
    fn parses_client_sentry_config() {
        let input = r#"
            CREATE SENTRY CLIENT sentry_main
              CONFIG {
                'dsn' = 'https://public@sentry.example.com/42',
                'timeout_ms' = 5000
              };
        "#;

        let tokens = to_tokens(input);
        let parsed = create_client_sentry_parser()
            .then_ignore(end())
            .parse(tokens.as_slice())
            .into_result()
            .expect("parse should succeed");

        assert_eq!(parsed.name.as_str(), "sentry_main");
        assert_eq!(parsed.config.len(), 2);
        assert_eq!(parsed.config[0].key, "dsn");
        assert_eq!(
            parsed.config[0].value,
            "https://public@sentry.example.com/42"
        );
        assert_eq!(parsed.config[1].key, "timeout_ms");
    }

    #[test]
    fn parses_client_s3_config() {
        let input = r#"
            CREATE S3 CLIENT s3_main
              CONFIG {
                'endpoint' = 'http://127.0.0.1:9900',
                'region' = 'us-east-1',
                'access_key_id' = 'rustfsadmin',
                'secret_access_key' = 'rustfsadmin',
                'path_style_access' = true
              };
        "#;

        let tokens = to_tokens(input);
        let parsed = create_client_s3_parser()
            .then_ignore(end())
            .parse(tokens.as_slice())
            .into_result()
            .expect("parse should succeed");

        assert_eq!(parsed.name.as_str(), "s3_main");
        assert_eq!(parsed.config.len(), 5);
        assert_eq!(parsed.config[0].key, "endpoint");
        assert_eq!(parsed.config[0].value, "http://127.0.0.1:9900");
        assert_eq!(parsed.config[4].key, "path_style_access");
        assert_eq!(parsed.config[4].value, "true");
    }

    #[test]
    fn parses_client_clickhouse_config() {
        let input = r#"
            CREATE CLICKHOUSE CLIENT clickhouse_main
              CONFIG {
                'addr' = 'http://127.0.0.1:8123',
                'database' = 'default',
                'timeout_ms' = 5000
              };
        "#;

        let tokens = to_tokens(input);
        let parsed = create_client_clickhouse_parser()
            .then_ignore(end())
            .parse(tokens.as_slice())
            .into_result()
            .expect("parse should succeed");

        assert_eq!(parsed.name.as_str(), "clickhouse_main");
        assert_eq!(parsed.config.len(), 3);
        assert_eq!(parsed.config[0].key, "addr");
        assert_eq!(parsed.config[0].value, "http://127.0.0.1:8123");
        assert_eq!(parsed.config[2].key, "timeout_ms");
        assert_eq!(parsed.config[2].value, "5000");
    }

    #[test]
    fn parses_client_postgres_config() {
        let input = r#"
            CREATE POSTGRES CLIENT postgres_main
              POOL SIZE MIN 2 MAX 8
              CONFIG {
                'addr' = 'postgresql://postgres:nervix@127.0.0.1:5432/postgres?sslmode=disable',
                'application_name' = 'nervix'
              };
        "#;

        let tokens = to_tokens(input);
        let parsed = create_client_postgres_parser()
            .then_ignore(end())
            .parse(tokens.as_slice())
            .into_result()
            .expect("parse should succeed");

        assert_eq!(parsed.name.as_str(), "postgres_main");
        assert_eq!(parsed.pool.minimum(), 2);
        assert_eq!(parsed.pool.maximum().get(), 8);
        assert_eq!(parsed.config.len(), 2);
        assert_eq!(parsed.config[0].key, "addr");
        assert_eq!(
            parsed.config[0].value,
            "postgresql://postgres:nervix@127.0.0.1:5432/postgres?sslmode=disable"
        );
        assert_eq!(parsed.config[1].key, "application_name");
    }

    #[test]
    fn parses_client_mysql_config() {
        let input = r#"
            CREATE MYSQL CLIENT mysql_main
              POOL SIZE MIN 0 MAX 8
              CONFIG {
                'addr' = 'mysql://nervix:nervix@127.0.0.1:3306/nervix',
                'application_name' = 'nervix'
              };
        "#;

        let tokens = to_tokens(input);
        let parsed = create_client_mysql_parser()
            .then_ignore(end())
            .parse(tokens.as_slice())
            .into_result()
            .expect("parse should succeed");

        assert_eq!(parsed.name.as_str(), "mysql_main");
        assert_eq!(parsed.pool.minimum(), 0);
        assert_eq!(parsed.pool.maximum().get(), 8);
        assert_eq!(parsed.config.len(), 2);
        assert_eq!(parsed.config[0].key, "addr");
        assert_eq!(
            parsed.config[0].value,
            "mysql://nervix:nervix@127.0.0.1:3306/nervix"
        );
        assert_eq!(parsed.config[1].key, "application_name");
    }

    #[test]
    fn parses_client_mongodb_config() {
        let input = r#"
            CREATE MONGODB CLIENT mongodb_main
              POOL SIZE MIN 3 MAX 3
              CONFIG {
                'addr' = 'mongodb://root:nervix@127.0.0.1:27017/nervix?authSource=admin',
                'database' = 'nervix'
              };
        "#;

        let tokens = to_tokens(input);
        let parsed = create_client_mongodb_parser()
            .then_ignore(end())
            .parse(tokens.as_slice())
            .into_result()
            .expect("parse should succeed");

        assert_eq!(parsed.name.as_str(), "mongodb_main");
        assert_eq!(parsed.pool.minimum(), 3);
        assert_eq!(parsed.pool.maximum().get(), 3);
        assert_eq!(parsed.config.len(), 2);
        assert_eq!(parsed.config[0].key, "addr");
        assert_eq!(
            parsed.config[0].value,
            "mongodb://root:nervix@127.0.0.1:27017/nervix?authSource=admin"
        );
        assert_eq!(parsed.config[1].key, "database");
    }

    #[test]
    fn parses_client_redis_config() {
        let input = r#"
            CREATE REDIS CLIENT redis_main
              POOL SIZE MIN 1 MAX 4
              CONFIG {
                'addr' = 'redis://127.0.0.1:6379/',
                'read_timeout_ms' = 5000
              };
        "#;

        let tokens = to_tokens(input);
        let parsed = create_client_redis_parser()
            .then_ignore(end())
            .parse(tokens.as_slice())
            .into_result()
            .expect("parse should succeed");

        assert_eq!(parsed.name.as_str(), "redis_main");
        assert_eq!(parsed.pool.minimum(), 1);
        assert_eq!(parsed.pool.maximum().get(), 4);
        assert_eq!(parsed.config.len(), 2);
        assert_eq!(parsed.config[0].key, "addr");
        assert_eq!(parsed.config[0].value, "redis://127.0.0.1:6379/");
        assert_eq!(parsed.config[1].key, "read_timeout_ms");
    }

    /// The rendered diagnostics of a statement the composed grammar rejects.
    fn statement_errors(input: &str) -> String {
        let error =
            crate::statement::parse_statement(input).expect_err("this statement must be rejected");
        error
            .current_context()
            .diagnostics()
            .iter()
            .map(|diagnostic| diagnostic.message.clone())
            .collect::<Vec<_>>()
            .join("; ")
    }

    #[test]
    fn pooled_client_requires_the_whole_pool_clause_in_the_declared_order() {
        assert!(
            statement_errors("CREATE POSTGRES CLIENT pg CONFIG { 'addr' = 'postgresql://h/d' };")
                .contains("expected POOL SIZE")
        );
        assert!(
            statement_errors(
                "CREATE POSTGRES CLIENT pg POOL SIZE MIN 2 CONFIG { 'addr' = 'postgresql://h/d' };"
            )
            .contains("expected MAX")
        );
        assert!(
            statement_errors(
                "CREATE POSTGRES CLIENT pg POOL SIZE MAX 8 MIN 2 CONFIG { 'addr' = \
                 'postgresql://h/d' };"
            )
            .contains("expected MIN")
        );
        assert!(
            statement_errors(
                "CREATE POSTGRES CLIENT pg MIN 2 MAX 8 CONFIG { 'addr' = 'postgresql://h/d' };"
            )
            .contains("expected POOL SIZE")
        );
        assert!(
            statement_errors(
                "CREATE POSTGRES CLIENT pg MOUNT dev_tls VERSION 1 POOL SIZE MIN 2 MAX 8 CONFIG { \
                 'addr' = 'postgresql://h/d' };"
            )
            .contains("expected POOL SIZE, found MOUNT")
        );
    }

    #[test]
    fn pooled_client_rejects_a_repeated_pool_clause() {
        assert!(
            statement_errors(
                "CREATE POSTGRES CLIENT pg POOL SIZE MIN 2 MIN 3 MAX 8 CONFIG { 'addr' = \
                 'postgresql://h/d' };"
            )
            .contains("expected MAX, found MIN")
        );
        assert!(
            statement_errors(
                "CREATE POSTGRES CLIENT pg POOL SIZE MIN 2 MAX 8 POOL SIZE MIN 3 MAX 9 CONFIG { \
                 'addr' = 'postgresql://h/d' };"
            )
            .contains("expected MOUNT | CONFIG, found POOL")
        );
    }

    #[test]
    fn pooled_client_rejects_a_maximum_of_zero() {
        assert!(
            statement_errors(
                "CREATE REDIS CLIENT rd POOL SIZE MIN 0 MAX 0 CONFIG { 'addr' = \
                 'redis://h:6379/0' };"
            )
            .contains("maximum pool size must be greater than zero")
        );
    }

    #[test]
    fn pooled_client_rejects_a_minimum_above_its_maximum() {
        assert!(
            statement_errors(
                "CREATE MYSQL CLIENT my POOL SIZE MIN 9 MAX 8 CONFIG { 'addr' = \
                 'mysql://h:3306/d' };"
            )
            .contains("minimum pool size 9 must not exceed maximum pool size 8")
        );
    }

    #[test]
    fn pooled_client_rejects_counts_outside_the_declared_range() {
        assert!(
            statement_errors(
                "CREATE MONGODB CLIENT mg POOL SIZE MIN 4294967296 MAX 8 CONFIG { 'addr' = \
                 'mongodb://h:27017/d' };"
            )
            .contains("invalid integer '4294967296'; expected 0 through 4294967295")
        );
        assert!(
            statement_errors(
                "CREATE MONGODB CLIENT mg POOL SIZE MIN 2 MAX 4294967296 CONFIG { 'addr' = \
                 'mongodb://h:27017/d' };"
            )
            .contains("invalid integer '4294967296'; expected 0 through 4294967295")
        );
    }

    #[test]
    fn pooled_client_rejects_fractional_and_negative_counts() {
        assert!(
            statement_errors(
                "CREATE REDIS CLIENT rd POOL SIZE MIN 1.5 MAX 4 CONFIG { 'addr' = \
                 'redis://h:6379/0' };"
            )
            .contains("invalid integer '1.5'")
        );
        assert!(
            statement_errors(
                "CREATE REDIS CLIENT rd POOL SIZE MIN 1 MAX -4 CONFIG { 'addr' = \
                 'redis://h:6379/0' };"
            )
            .contains("expected max_pool_size, found -")
        );
    }

    #[test]
    fn pool_bounds_do_not_leak_into_client_types_without_a_pool() {
        assert!(
            statement_errors(
                "CREATE KAFKA CLIENT kafka_main POOL SIZE MIN 2 MAX 8 CONFIG { \
                 'bootstrap.servers' = '127.0.0.1:9092' };"
            )
            .contains("expected MOUNT | CONFIG, found POOL")
        );
    }

    #[test]
    fn completion_walks_the_pool_clause_only_for_pooled_client_types() {
        let after_type = "CREATE POSTGRES CLIENT pg ";
        let suggestions = crate::statement::suggest_statement(after_type, after_type.len());
        assert_eq!(suggestions, vec!["POOL SIZE".to_string()]);

        let after_clause_head = "CREATE POSTGRES CLIENT pg POOL SIZE ";
        let suggestions =
            crate::statement::suggest_statement(after_clause_head, after_clause_head.len());
        assert_eq!(suggestions, vec!["MIN".to_string()]);

        let after_minimum = "CREATE POSTGRES CLIENT pg POOL SIZE MIN 2 ";
        let suggestions = crate::statement::suggest_statement(after_minimum, after_minimum.len());
        assert_eq!(suggestions, vec!["MAX".to_string()]);

        let after_maximum = "CREATE POSTGRES CLIENT pg POOL SIZE MIN 2 MAX 8 ";
        let suggestions = crate::statement::suggest_statement(after_maximum, after_maximum.len());
        assert!(suggestions.contains(&"MOUNT".to_string()));
        assert!(suggestions.contains(&"CONFIG".to_string()));
        assert!(!suggestions.contains(&"POOL SIZE".to_string()));
        assert!(!suggestions.contains(&"MIN".to_string()));
        assert!(!suggestions.contains(&"MAX".to_string()));
    }

    #[test]
    fn completion_expects_a_count_after_each_pool_bound_keyword() {
        let after_min = "CREATE REDIS CLIENT rd POOL SIZE MIN ";
        let suggestions = crate::statement::suggest_statement(after_min, after_min.len());
        assert_eq!(suggestions, vec!["min_pool_size".to_string()]);

        let after_max = "CREATE REDIS CLIENT rd POOL SIZE MIN 1 MAX ";
        let suggestions = crate::statement::suggest_statement(after_max, after_max.len());
        assert_eq!(suggestions, vec!["max_pool_size".to_string()]);
    }

    #[test]
    fn completion_does_not_offer_pool_bounds_after_an_unpooled_client_type() {
        let after_type = "CREATE KAFKA CLIENT kafka_main ";
        let suggestions = crate::statement::suggest_statement(after_type, after_type.len());
        assert!(suggestions.contains(&"CONFIG".to_string()));
        assert!(!suggestions.contains(&"POOL SIZE".to_string()));
    }

    #[test]
    fn parses_client_mqtt_config() {
        let input = r#"
            CREATE MQTT CLIENT mqtt_main
              CONFIG {
                'addr' = 'mqtt://127.0.0.1:1883',
                'client_id' = 'nervix-mqtt'
              };
        "#;

        let tokens = to_tokens(input);
        let parsed = create_client_mqtt_parser()
            .then_ignore(end())
            .parse(tokens.as_slice())
            .into_result()
            .expect("parse should succeed");

        assert_eq!(parsed.name.as_str(), "mqtt_main");
        assert_eq!(parsed.config.len(), 2);
        assert_eq!(parsed.config[0].key, "addr");
        assert_eq!(parsed.config[0].value, "mqtt://127.0.0.1:1883");
        assert_eq!(parsed.config[1].key, "client_id");
    }

    #[test]
    fn parses_client_prometheus_config() {
        let input = r#"
            CREATE PROMETHEUS CLIENT prom_main
              CONFIG {
                'addr' = 'http://127.0.0.1:9090',
                'timeout_ms' = 5000
              };
        "#;

        let tokens = to_tokens(input);
        let parsed = create_client_prometheus_parser()
            .then_ignore(end())
            .parse(tokens.as_slice())
            .into_result()
            .expect("parse should succeed");

        assert_eq!(parsed.name.as_str(), "prom_main");
        assert_eq!(parsed.config.len(), 2);
        assert_eq!(parsed.config[0].key, "addr");
        assert_eq!(parsed.config[0].value, "http://127.0.0.1:9090");
        assert_eq!(parsed.config[1].key, "timeout_ms");
    }

    #[test]
    fn parses_client_zeromq_config() {
        let input = r#"
            CREATE ZEROMQ CLIENT zmq_main
              CONFIG {
                'addr' = 'tcp://127.0.0.1:5555',
                'bind' = true
              };
        "#;

        let tokens = to_tokens(input);
        let parsed = create_client_zeromq_parser()
            .then_ignore(end())
            .parse(tokens.as_slice())
            .into_result()
            .expect("parse should succeed");

        assert_eq!(parsed.name.as_str(), "zmq_main");
        assert_eq!(parsed.config.len(), 2);
        assert_eq!(parsed.config[0].key, "addr");
        assert_eq!(parsed.config[1].value, "true");
    }

    #[test]
    fn parses_client_sqs_config() {
        let input = r#"
            CREATE SQS CLIENT sqs_main
              CONFIG {
                'endpoint' = 'http://127.0.0.1:9324',
                'region' = 'us-east-1'
              };
        "#;

        let tokens = to_tokens(input);
        let parsed = create_client_sqs_parser()
            .then_ignore(end())
            .parse(tokens.as_slice())
            .into_result()
            .expect("parse should succeed");

        assert_eq!(parsed.name.as_str(), "sqs_main");
        assert_eq!(parsed.config.len(), 2);
        assert_eq!(parsed.config[0].key, "endpoint");
        assert_eq!(parsed.config[1].value, "us-east-1");
    }

    #[test]
    fn parses_client_websockets_config() {
        let input = r#"
            CREATE WEBSOCKETS CLIENT ws_main
              CONFIG {
                'endpoint' = 'wss://api.example.com/ws',
                'subprotocol' = 'notifications'
              };
        "#;

        let tokens = to_tokens(input);
        let parsed = create_client_websockets_parser()
            .then_ignore(end())
            .parse(tokens.as_slice())
            .into_result()
            .expect("parse should succeed");

        assert_eq!(parsed.name.as_str(), "ws_main");
        assert_eq!(parsed.config.len(), 2);
        assert_eq!(parsed.config[0].key, "endpoint");
        assert_eq!(parsed.config[0].value, "wss://api.example.com/ws");
        assert_eq!(parsed.config[1].key, "subprotocol");
        assert_eq!(parsed.signaling_protocol, None);
    }

    #[test]
    fn parses_client_websockets_signaling_protocol() {
        let input = r#"
            CREATE WEBSOCKETS CLIENT ws_main
              WITH SIGNALING PROTOCOL binance_style
              CONFIG {
                'endpoint' = 'wss://api.example.com/ws'
              };
        "#;

        let tokens = to_tokens(input);
        let parsed = create_client_websockets_parser()
            .then_ignore(end())
            .parse(tokens.as_slice())
            .into_result()
            .expect("parse should succeed");

        assert_eq!(
            parsed
                .signaling_protocol
                .as_ref()
                .map(nervix_models::SignalingProtocolName::as_str),
            Some("binance_style")
        );
    }

    #[test]
    fn parses_websocket_signaling_protocol_before_versioned_mount() {
        let input = "CREATE WEBSOCKETS CLIENT ws_main WITH SIGNALING PROTOCOL binance_style MOUNT \
                     ws_tls VERSION 2 CONFIG { 'endpoint' = 'wss://api.example.com/ws' };";
        let tokens = to_tokens(input);
        let parsed = create_client_websockets_parser()
            .then_ignore(end())
            .parse(tokens.as_slice())
            .into_result()
            .expect("parse should succeed");

        let mount = parsed.mount.as_ref().expect("mount is present");
        assert_eq!(mount.resource.as_str(), "ws_tls");
        assert_eq!(mount.version, RequestedResourceVersion::Number(2));
    }

    #[test]
    fn parses_client_nats_config() {
        let input = r#"
            CREATE NATS CLIENT nats_main
              CONFIG {
                'addr' = 'nats://127.0.0.1:4222',
                'name' = 'nervix-nats'
              };
        "#;

        let tokens = to_tokens(input);
        let parsed = create_client_nats_parser()
            .then_ignore(end())
            .parse(tokens.as_slice())
            .into_result()
            .expect("parse should succeed");

        assert_eq!(parsed.name.as_str(), "nats_main");
        assert_eq!(parsed.config.len(), 2);
        assert_eq!(parsed.config[0].value, "nats://127.0.0.1:4222");
    }
}
