use chumsky::prelude::*;
use nervix_models::{
    CreateClientAzureBlob, CreateClientClickHouse, CreateClientGcs, CreateClientHttp,
    CreateClientIcebergRest, CreateClientKafka, CreateClientKinesis, CreateClientMongoDb,
    CreateClientMqtt, CreateClientMySql, CreateClientNats, CreateClientPostgres,
    CreateClientPrometheus, CreateClientPulsar, CreateClientRabbitMq, CreateClientRedis,
    CreateClientS3, CreateClientSqs, CreateClientWebsockets, CreateClientZeroMq, CreateStatement,
    KafkaConfigEntry,
};

use crate::{
    lexer::{Identifier, Token},
    parser_support::{
        ParseError, client_name, current_word_prefix, if_not_exists_clause, into_parse_error, kw,
        lex_input, signaling_protocol_clause, string_lit, suggestions_from_errors, tok, word_raw,
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

fn config_key<'src>()
-> impl Parser<'src, &'src [Token], String, extra::Err<ParseError<'src>>> + Clone {
    string_lit().labelled("config_key")
}

fn config_value<'src>()
-> impl Parser<'src, &'src [Token], String, extra::Err<ParseError<'src>>> + Clone {
    scalar_value().labelled("config_value")
}

fn config_entry<'src>()
-> impl Parser<'src, &'src [Token], KafkaConfigEntry, extra::Err<ParseError<'src>>> + Clone {
    config_key()
        .then_ignore(tok(Token::Eq))
        .then(config_value())
        .map(|(key, value)| KafkaConfigEntry { key, value })
}

fn client_mount<'src>()
-> impl Parser<'src, &'src [Token], Option<nervix_models::Identifier>, extra::Err<ParseError<'src>>>
+ Clone {
    kw(Identifier::Mount)
        .ignore_then(client_name().labelled("resource_name"))
        .or_not()
}

fn create_client_parser<'src, T>(
    client_type: Identifier,
    build: impl Fn(
        nervix_models::Identifier,
        Option<nervix_models::Identifier>,
        Vec<KafkaConfigEntry>,
    ) -> T
    + Clone
    + 'src,
) -> impl Parser<'src, &'src [Token], CreateStatement<T>, extra::Err<ParseError<'src>>> + Clone {
    kw(Identifier::Create)
        .ignore_then(if_not_exists_clause())
        .then_ignore(kw(Identifier::Client))
        .then(client_name())
        .then_ignore(kw(Identifier::Type))
        .then_ignore(kw(client_type))
        .then(client_mount())
        .then_ignore(kw(Identifier::Config))
        .then(transport_config())
        .then_ignore(tok(Token::Semicolon).or_not())
        .map(move |(((if_not_exists, name), mount), config)| {
            CreateStatement::new(build(name, mount, config), if_not_exists)
        })
}

pub fn create_client_kafka_parser<'src>()
-> impl Parser<'src, &'src [Token], CreateStatement<CreateClientKafka>, extra::Err<ParseError<'src>>>
+ Clone {
    create_client_parser(Identifier::Kafka, |name, mount, config| CreateClientKafka {
        name,
        mount,
        config,
    })
}

pub fn create_client_pulsar_parser<'src>()
-> impl Parser<'src, &'src [Token], CreateStatement<CreateClientPulsar>, extra::Err<ParseError<'src>>>
+ Clone {
    create_client_parser(Identifier::Pulsar, |name, mount, config| {
        CreateClientPulsar {
            name,
            mount,
            config,
        }
    })
}

pub fn create_client_http_parser<'src>()
-> impl Parser<'src, &'src [Token], CreateStatement<CreateClientHttp>, extra::Err<ParseError<'src>>>
+ Clone {
    create_client_parser(Identifier::Http, |name, mount, config| CreateClientHttp {
        name,
        mount,
        config,
    })
}

pub fn create_client_kinesis_parser<'src>()
-> impl Parser<'src, &'src [Token], CreateStatement<CreateClientKinesis>, extra::Err<ParseError<'src>>>
+ Clone {
    create_client_parser(Identifier::Kinesis, |name, mount, config| {
        CreateClientKinesis {
            name,
            mount,
            config,
        }
    })
}

pub fn create_client_clickhouse_parser<'src>() -> impl Parser<
    'src,
    &'src [Token],
    CreateStatement<CreateClientClickHouse>,
    extra::Err<ParseError<'src>>,
> + Clone {
    create_client_parser(Identifier::Clickhouse, |name, mount, config| {
        CreateClientClickHouse {
            name,
            mount,
            config,
        }
    })
}

pub fn create_client_postgres_parser<'src>()
-> impl Parser<'src, &'src [Token], CreateStatement<CreateClientPostgres>, extra::Err<ParseError<'src>>>
+ Clone {
    create_client_parser(Identifier::Postgres, |name, mount, config| {
        CreateClientPostgres {
            name,
            mount,
            config,
        }
    })
}

pub fn create_client_mysql_parser<'src>()
-> impl Parser<'src, &'src [Token], CreateStatement<CreateClientMySql>, extra::Err<ParseError<'src>>>
+ Clone {
    create_client_parser(Identifier::Mysql, |name, mount, config| CreateClientMySql {
        name,
        mount,
        config,
    })
}

pub fn create_client_mongodb_parser<'src>()
-> impl Parser<'src, &'src [Token], CreateStatement<CreateClientMongoDb>, extra::Err<ParseError<'src>>>
+ Clone {
    create_client_parser(Identifier::Mongodb, |name, mount, config| {
        CreateClientMongoDb {
            name,
            mount,
            config,
        }
    })
}

pub fn create_client_rabbitmq_parser<'src>()
-> impl Parser<'src, &'src [Token], CreateStatement<CreateClientRabbitMq>, extra::Err<ParseError<'src>>>
+ Clone {
    create_client_parser(Identifier::Rabbitmq, |name, mount, config| {
        CreateClientRabbitMq {
            name,
            mount,
            config,
        }
    })
}

pub fn create_client_redis_parser<'src>()
-> impl Parser<'src, &'src [Token], CreateStatement<CreateClientRedis>, extra::Err<ParseError<'src>>>
+ Clone {
    create_client_parser(Identifier::Redis, |name, mount, config| CreateClientRedis {
        name,
        mount,
        config,
    })
}

pub fn create_client_mqtt_parser<'src>()
-> impl Parser<'src, &'src [Token], CreateStatement<CreateClientMqtt>, extra::Err<ParseError<'src>>>
+ Clone {
    create_client_parser(Identifier::Mqtt, |name, mount, config| CreateClientMqtt {
        name,
        mount,
        config,
    })
}

pub fn create_client_nats_parser<'src>()
-> impl Parser<'src, &'src [Token], CreateStatement<CreateClientNats>, extra::Err<ParseError<'src>>>
+ Clone {
    create_client_parser(Identifier::Nats, |name, mount, config| CreateClientNats {
        name,
        mount,
        config,
    })
}

pub fn create_client_prometheus_parser<'src>() -> impl Parser<
    'src,
    &'src [Token],
    CreateStatement<CreateClientPrometheus>,
    extra::Err<ParseError<'src>>,
> + Clone {
    create_client_parser(Identifier::Prometheus, |name, mount, config| {
        CreateClientPrometheus {
            name,
            mount,
            config,
        }
    })
}

pub fn create_client_zeromq_parser<'src>()
-> impl Parser<'src, &'src [Token], CreateStatement<CreateClientZeroMq>, extra::Err<ParseError<'src>>>
+ Clone {
    create_client_parser(Identifier::Zeromq, |name, mount, config| {
        CreateClientZeroMq {
            name,
            mount,
            config,
        }
    })
}

pub fn create_client_sqs_parser<'src>()
-> impl Parser<'src, &'src [Token], CreateStatement<CreateClientSqs>, extra::Err<ParseError<'src>>>
+ Clone {
    create_client_parser(Identifier::Sqs, |name, mount, config| CreateClientSqs {
        name,
        mount,
        config,
    })
}

pub fn create_client_s3_parser<'src>()
-> impl Parser<'src, &'src [Token], CreateStatement<CreateClientS3>, extra::Err<ParseError<'src>>>
+ Clone {
    create_client_parser(Identifier::S3, |name, mount, config| CreateClientS3 {
        name,
        mount,
        config,
    })
}

pub fn create_client_gcs_parser<'src>()
-> impl Parser<'src, &'src [Token], CreateStatement<CreateClientGcs>, extra::Err<ParseError<'src>>>
+ Clone {
    create_client_parser(Identifier::Gcs, |name, mount, config| CreateClientGcs {
        name,
        mount,
        config,
    })
}

pub fn create_client_azure_blob_parser<'src>()
-> impl Parser<'src, &'src [Token], CreateStatement<CreateClientAzureBlob>, extra::Err<ParseError<'src>>>
+ Clone {
    create_client_parser(Identifier::AzureBlob, |name, mount, config| {
        CreateClientAzureBlob {
            name,
            mount,
            config,
        }
    })
}

pub fn create_client_iceberg_rest_parser<'src>() -> impl Parser<
    'src,
    &'src [Token],
    CreateStatement<CreateClientIcebergRest>,
    extra::Err<ParseError<'src>>,
> + Clone {
    create_client_parser(Identifier::IcebergRest, |name, mount, config| {
        CreateClientIcebergRest {
            name,
            mount,
            config,
        }
    })
}

pub fn create_client_websockets_parser<'src>() -> impl Parser<
    'src,
    &'src [Token],
    CreateStatement<CreateClientWebsockets>,
    extra::Err<ParseError<'src>>,
> + Clone {
    kw(Identifier::Create)
        .ignore_then(if_not_exists_clause())
        .then_ignore(kw(Identifier::Client))
        .then(client_name())
        .then_ignore(kw(Identifier::Type))
        .then_ignore(kw(Identifier::Websockets))
        .then(signaling_protocol_clause().or_not())
        .then(client_mount())
        .then_ignore(kw(Identifier::Config))
        .then(transport_config())
        .then_ignore(tok(Token::Semicolon).or_not())
        .map(
            |((((if_not_exists, name), signaling_protocol), mount), config)| {
                CreateStatement::new(
                    CreateClientWebsockets {
                        name,
                        mount,
                        signaling_protocol,
                        config,
                    },
                    if_not_exists,
                )
            },
        )
}

fn transport_config<'src>()
-> impl Parser<'src, &'src [Token], Vec<KafkaConfigEntry>, extra::Err<ParseError<'src>>> + Clone {
    config_entry()
        .separated_by(tok(Token::Comma))
        .allow_trailing()
        .collect::<Vec<_>>()
        .delimited_by(tok(Token::LBrace), tok(Token::RBrace))
}

pub fn parse_create_client_kafka_tokens(
    tokens: &[Token],
) -> Result<CreateStatement<CreateClientKafka>, Vec<ParseError<'_>>> {
    let out = create_client_kafka_parser()
        .then_ignore(end())
        .parse(tokens);
    if out.has_errors() {
        Err(out.into_errors())
    } else {
        Ok(out
            .into_output()
            .expect("successful parse must have output"))
    }
}

pub fn parse_create_client_kafka(
    input: &str,
) -> Result<CreateStatement<CreateClientKafka>, ParseFromSourceError> {
    let (source, spanned_tokens, tokens) = lex_input(input)?;
    parse_create_client_kafka_tokens(&tokens)
        .map_err(|errs| into_parse_error(source, &spanned_tokens, input.len(), errs))
}

pub fn suggest_create_client_kafka(input: &str, cursor: usize) -> Vec<String> {
    let safe_cursor = cursor.min(input.len());
    let prefix_src = &input[..safe_cursor];
    let prefix = current_word_prefix(prefix_src);

    let (_, _, tokens) = match lex_input(prefix_src) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };

    let out = create_client_kafka_parser()
        .then_ignore(end())
        .parse(tokens.as_slice());
    if !out.has_errors() {
        return Vec::new();
    }

    suggestions_from_errors(out.into_errors(), &prefix)
}

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
            CREATE CLIENT kafka_main
              TYPE KAFKA
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
            CREATE CLIENT kafka_tls
              TYPE KAFKA
              MOUNT dev_tls
              CONFIG {
                'ssl.ca.location' = '{{dev_tls}}/ca.pem'
              };
        "#;

        let tokens = to_tokens(input);
        let parsed = parse_create_client_kafka_tokens(&tokens).expect("parse should succeed");

        assert_eq!(parsed.name.as_str(), "kafka_tls");
        assert_eq!(
            parsed.mount.as_ref().map(nervix_models::Identifier::as_str),
            Some("dev_tls")
        );
        assert_eq!(parsed.config[0].value, "{{dev_tls}}/ca.pem");
    }

    #[test]
    fn parses_client_gcs_config() {
        let input = r#"
            CREATE CLIENT gcs_main
              TYPE GCS
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
            CREATE CLIENT azure_main
              TYPE AZURE_BLOB
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
            CREATE CLIENT iceberg_catalog
              TYPE ICEBERG_REST
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
            CREATE CLIENT pulsar_main
              TYPE PULSAR
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
        let tokens = to_tokens("CREATE CLIENT kafka_main TYPE KAFKA CONFIG;");
        let errs = parse_create_client_kafka_tokens(&tokens).expect_err("must fail");
        assert!(!errs.is_empty());
    }

    #[test]
    fn fails_when_mount_has_no_resource_name() {
        let tokens = to_tokens("CREATE CLIENT kafka_main TYPE KAFKA MOUNT CONFIG { 'a' = 'b' };");
        let errs = parse_create_client_kafka_tokens(&tokens).expect_err("must fail");
        assert!(!errs.is_empty());
    }

    #[test]
    fn mount_context_suggests_config_without_type_leakage() {
        let input = "CREATE CLIENT kafka_main TYPE KAFKA MOUNT dev_tls ";
        let suggestions = suggest_create_client_kafka(input, input.len());
        assert!(suggestions.contains(&"CONFIG".to_string()));
        assert!(!suggestions.contains(&"HTTP".to_string()));
        assert!(!suggestions.contains(&"RABBITMQ".to_string()));
    }

    #[test]
    fn parses_client_rabbitmq_config() {
        let input = r#"
            CREATE CLIENT rabbit_main
              TYPE RABBITMQ
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
            CREATE CLIENT http_main
              TYPE HTTP
              CONFIG {
                'endpoint' = 'https://api.example.com/events',
                'method' = 'POST'
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
        assert_eq!(parsed.config[1].key, "method");
    }

    #[test]
    fn parses_client_kinesis_config() {
        let input = r#"
            CREATE CLIENT kinesis_main
              TYPE KINESIS
              CONFIG {
                'endpoint' = 'http://127.0.0.1:4566',
                'region' = 'us-east-1',
                'start_position' = 'trim_horizon'
              };
        "#;

        let tokens = to_tokens(input);
        let parsed = create_client_kinesis_parser()
            .then_ignore(end())
            .parse(tokens.as_slice())
            .into_result()
            .expect("parse should succeed");

        assert_eq!(parsed.name.as_str(), "kinesis_main");
        assert_eq!(parsed.config.len(), 3);
        assert_eq!(parsed.config[0].key, "endpoint");
        assert_eq!(parsed.config[0].value, "http://127.0.0.1:4566");
        assert_eq!(parsed.config[2].key, "start_position");
        assert_eq!(parsed.config[2].value, "trim_horizon");
    }

    #[test]
    fn parses_client_s3_config() {
        let input = r#"
            CREATE CLIENT s3_main
              TYPE S3
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
            CREATE CLIENT clickhouse_main
              TYPE CLICKHOUSE
              CONFIG {
                'addr' = 'http://127.0.0.1:8123',
                'database' = 'default'
              };
        "#;

        let tokens = to_tokens(input);
        let parsed = create_client_clickhouse_parser()
            .then_ignore(end())
            .parse(tokens.as_slice())
            .into_result()
            .expect("parse should succeed");

        assert_eq!(parsed.name.as_str(), "clickhouse_main");
        assert_eq!(parsed.config.len(), 2);
        assert_eq!(parsed.config[0].key, "addr");
        assert_eq!(parsed.config[0].value, "http://127.0.0.1:8123");
    }

    #[test]
    fn parses_client_postgres_config() {
        let input = r#"
            CREATE CLIENT postgres_main
              TYPE POSTGRES
              CONFIG {
                'addr' = 'host=127.0.0.1 port=5432 user=postgres password=nervix dbname=postgres',
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
        assert_eq!(parsed.config.len(), 2);
        assert_eq!(parsed.config[0].key, "addr");
        assert_eq!(
            parsed.config[0].value,
            "host=127.0.0.1 port=5432 user=postgres password=nervix dbname=postgres"
        );
        assert_eq!(parsed.config[1].key, "application_name");
    }

    #[test]
    fn parses_client_mysql_config() {
        let input = r#"
            CREATE CLIENT mysql_main
              TYPE MYSQL
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
            CREATE CLIENT mongodb_main
              TYPE MONGODB
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
            CREATE CLIENT redis_main
              TYPE REDIS
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
        assert_eq!(parsed.config.len(), 2);
        assert_eq!(parsed.config[0].key, "addr");
        assert_eq!(parsed.config[0].value, "redis://127.0.0.1:6379/");
        assert_eq!(parsed.config[1].key, "read_timeout_ms");
    }

    #[test]
    fn parses_client_mqtt_config() {
        let input = r#"
            CREATE CLIENT mqtt_main
              TYPE MQTT
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
            CREATE CLIENT prom_main
              TYPE PROMETHEUS
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
            CREATE CLIENT zmq_main
              TYPE ZEROMQ
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
            CREATE CLIENT sqs_main
              TYPE SQS
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
            CREATE CLIENT ws_main
              TYPE WEBSOCKETS
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
            CREATE CLIENT ws_main
              TYPE WEBSOCKETS WITH SIGNALING PROTOCOL binance_style
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
                .map(nervix_models::Identifier::as_str),
            Some("binance_style")
        );
    }

    #[test]
    fn parses_client_nats_config() {
        let input = r#"
            CREATE CLIENT nats_main
              TYPE NATS
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
