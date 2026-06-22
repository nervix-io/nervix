Feature: HTTP ingestor specific filter-map logic
  Scenario Outline: HTTP ingestor filter-map rejects schema mismatches introduced by SET
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When the ingestor logic fixture "http_endpoint" fails to start with output schema "input" and program
      """
      SET logic_notifications.normalized = lower(trim(message.raw)), logic_notifications.amount = message.amount + 1
      UNSET logic_notifications.raw
      """
    Then the ingestor logic expectation "compile_error" is observed

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: HTTP ingestor filter-map supports chained calls and nested boolean expressions
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When the ingestor logic fixture "http_endpoint" starts with output schema "function_matrix" and program
      """
      SET logic_notifications.amount_abs = abs(-message.amount),
          logic_notifications.trimmed = trim(message.raw),
          logic_notifications.lowered = lower(trim(message.raw)),
          logic_notifications.uppered = upper(trim(message.raw)),
          logic_notifications.raw_len = length(trim(message.raw)),
          logic_notifications.contains_keep = contains(lower(trim(message.raw)), 'eep'),
          logic_notifications.starts_keep = starts_with(lower(trim(message.raw)), 'keep'),
          logic_notifications.ends_me = ends_with(lower(trim(message.raw)), 'me'),
          logic_notifications.fallback = coalesce(nullif(lower(trim(message.raw)), 'keepme'), 'fallback'),
          logic_notifications.was_keep = is_null(nullif(lower(trim(message.raw)), 'keepme'))
      UNSET logic_notifications.active, logic_notifications.amount, logic_notifications.raw
      WHERE message.active AND message.amount > 5 AND NOT starts_with(lower(trim(message.raw)), 'drop') AND (contains(lower(message.raw), 'keep') OR ends_with(lower(trim(message.raw)), 'me'))
      """
    And the ingestor logic transport "http_endpoint" delivers payload fixture "function_matrix_message"
    Then the relay subscription receives a payload
      """
      "tenant":"acme"
      """
    And the last relay subscription payload contains key fragment '{"tenant":"acme"}'
    And the last relay subscription payload does not contain "raw\""
    And the last relay subscription payload does not contain "fallback\":null"
    And the last relay subscription payload does not contain "was_keep\":false"
    And the last relay subscription payload contains
      """
      "amount_abs":7
      "trimmed":"KeepMe"
      "lowered":"keepme"
      "uppered":"KEEPME"
      "raw_len":6
      "contains_keep":true
      "starts_keep":true
      "ends_me":true
      "fallback":"fallback"
      "was_keep":true
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: HTTP ingestor filter-map supports arithmetic operators with nested conditions
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When the ingestor logic fixture "http_endpoint" starts with output schema "arithmetic_matrix" and program
      """
      SET logic_notifications.parsed = message.raw AS INT64,
          logic_notifications.sum = message.amount + (message.raw AS INT64),
          logic_notifications.difference = message.amount - (message.raw AS INT64),
          logic_notifications.product = message.amount * (message.raw AS INT64),
          logic_notifications.quotient = message.amount / (message.raw AS INT64),
          logic_notifications.remainder = message.amount % (message.raw AS INT64),
          logic_notifications.complex = (message.amount + (message.raw AS INT64)) * (message.amount - (message.raw AS INT64)),
          logic_notifications.comparison = (message.amount / (message.raw AS INT64)) > ((message.raw AS INT64) - 4),
          logic_notifications.chained = lower(trim(upper(message.tenant)))
      UNSET logic_notifications.active, logic_notifications.amount, logic_notifications.raw
      WHERE (message.active AND message.amount < 0) OR (NOT message.active AND ((message.raw AS INT64) < message.amount AND message.amount > 10))
      """
    And the ingestor logic transport "http_endpoint" delivers payload fixture "arithmetic_message"
    Then the relay subscription receives a payload
      """
      "tenant":"acme"
      """
    And the last relay subscription payload contains key fragment '{"tenant":"acme"}'
    And the last relay subscription payload does not contain "raw\""
    And the last relay subscription payload contains
      """
      "parsed":6
      "sum":26
      "difference":14
      "product":120
      "quotient":3
      "remainder":2
      "complex":364
      "comparison":true
      "chained":"acme"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: HTTP ingestor filter-map supports contextual, text, and regular-expression builtins
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When the ingestor logic fixture "http_endpoint" starts with output schema "extended_builtin_matrix" and program
      """
      SET logic_notifications.now_text = now() AS STRING,
          logic_notifications.uuid4 = uuid_v4(),
          logic_notifications.uuid7 = uuid_v7(),
          logic_notifications.bit_len = bit_length(trim(message.raw)),
          logic_notifications.ascii_value = ascii(trim(message.raw)),
          logic_notifications.btrimmed = btrim(message.raw),
          logic_notifications.char_len = char_length(message.raw),
          logic_notifications.joined = concat('he', '-', message.tenant),
          logic_notifications.titled = initcap(message.raw),
          logic_notifications.lefted = left(trim(message.raw), 2),
          logic_notifications.lowered = lower(trim(message.raw)),
          logic_notifications.lpaded = lpad('he', 7, 'xy'),
          logic_notifications.ltrimmed = ltrim(message.raw),
          logic_notifications.digest = md5('he'),
          logic_notifications.repeated = repeat('he', 2),
          logic_notifications.replaced = replace(trim(message.raw), 'he', 'HE'),
          logic_notifications.reversed = reverse('he'),
          logic_notifications.righted = right(trim(message.raw), 2),
          logic_notifications.rpaded = rpad('he', 7, 'xy'),
          logic_notifications.rtrimmed = rtrim(message.raw),
          logic_notifications.part = split_part('alpha.beta.gamma', '.', 2),
          logic_notifications.starts = starts_with(lower(trim(message.raw)), 'he'),
          logic_notifications.pos = strpos(trim(message.raw), 'he'),
          logic_notifications.piece = substr(trim(message.raw), 2, 3),
          logic_notifications.hexed = to_hex(255),
          logic_notifications.translated = translate('he', 'he', 'HE'),
          logic_notifications.trimmed2 = trim(message.raw),
          logic_notifications.uppered = upper(message.tenant),
          logic_notifications.regex_ok = regexp_like(trim(message.raw), 'h[a-z]+'),
          logic_notifications.regex_replaced = regexp_replace(trim(message.raw), 'h[a-z]+', 'XX'),
          logic_notifications.regex_piece = regexp_substr(message.raw, 'h[a-z]+')
      UNSET logic_notifications.active, logic_notifications.amount, logic_notifications.raw
      WHERE message.tenant = 'acme' AND message.active AND starts_with(lower(trim(message.raw)), 'he')
      """
    And the ingestor logic transport "http_endpoint" delivers payload fixture "extended_builtin_message"
    Then the relay subscription receives a payload
      """
      "tenant":"acme"
      """
    And the last relay subscription payload contains key fragment '{"tenant":"acme"}'
    And the last relay subscription payload does not contain "raw\""
    And the last relay subscription payload contains
      """
      "now_text":"
      "uuid4":"
      "uuid7":"
      """
    And the last relay subscription payload contains
      """
      "bit_len":88
      "ascii_value":104
      "btrimmed":"hello.world"
      "char_len":15
      "joined":"he-acme"
      "titled":"  Hello.World  "
      "lefted":"he"
      "lowered":"hello.world"
      "lpaded":"xyxyxhe"
      "ltrimmed":"hello.world  "
      "digest":"6f96cfdfe5ccc627cadf24b41725caa4"
      "repeated":"hehe"
      "replaced":"HEllo.world"
      "reversed":"eh"
      "righted":"ld"
      "rpaded":"hexyxyx"
      "rtrimmed":"  hello.world"
      "part":"beta"
      "starts":true
      "pos":1
      "piece":"ell"
      "hexed":"ff"
      "translated":"HE"
      "trimmed2":"hello.world"
      "uppered":"ACME"
      "regex_ok":true
      "regex_replaced":"XX.world"
      "regex_piece":"hello"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: HTTP ingestor filter-map supports literals, casts, and unary expressions
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When the ingestor logic fixture "http_endpoint" starts with output schema "cast_matrix" and program
      """
      SET logic_notifications.parsed = message.raw AS INT64,
          logic_notifications.amount_text = message.amount AS STRING,
          logic_notifications.amount_float = message.amount AS FLOAT64,
          logic_notifications.truthy = 1 AS BOOLEAN,
          logic_notifications.not_active = NOT message.active,
          logic_notifications.literal_bool = true,
          logic_notifications.literal_float = 3.5,
          logic_notifications.literal_int = 9,
          logic_notifications.label = 'ready',
          logic_notifications.is_exact = message.raw = (message.amount AS STRING),
          logic_notifications.negated = -message.amount
      UNSET logic_notifications.active, logic_notifications.amount, logic_notifications.raw
      WHERE message.tenant = 'acme' AND NOT message.active
      """
    And the ingestor logic transport "http_endpoint" delivers payload fixture "cast_matrix_message"
    Then the relay subscription receives a payload
      """
      "tenant":"acme"
      """
    And the last relay subscription payload contains key fragment '{"tenant":"acme"}'
    And the last relay subscription payload does not contain "raw\""
    And the last relay subscription payload contains
      """
      "parsed":42
      "amount_text":"42"
      "amount_float":42.0
      "truthy":true
      "not_active":true
      "literal_bool":true
      "literal_float":3.5
      "literal_int":9
      "label":"ready"
      "is_exact":true
      "negated":-42
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: HTTP ingestor filter-map supports transcendental and rounding math builtins
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When the ingestor logic fixture "http_endpoint" starts with output schema "math_builtin_matrix" and program
      """
      SET logic_notifications.absolute = abs(-message.amount),
          logic_notifications.acos_value = acos(0.5),
          logic_notifications.asin_value = asin(0.5),
          logic_notifications.atan_value = atan(2.0),
          logic_notifications.ceil_value = ceil(-1.75),
          logic_notifications.cos_value = cos(0.5),
          logic_notifications.exp_value = exp(1.0),
          logic_notifications.floor_value = floor(-1.75),
          logic_notifications.ln_value = ln(2.0),
          logic_notifications.log_value = log(100.0),
          logic_notifications.log_base_value = log(2.0, 100.0),
          logic_notifications.pow_value = pow(2.0, 3.0),
          logic_notifications.round_value = round(1.6),
          logic_notifications.sqrt_value = sqrt(9.0),
          logic_notifications.tan_value = tan(0.5)
      UNSET logic_notifications.active, logic_notifications.amount, logic_notifications.raw
      WHERE message.tenant = 'acme' AND message.active
      """
    And the ingestor logic transport "http_endpoint" delivers payload fixture "math_builtin_message"
    Then the relay subscription receives a payload
      """
      "tenant":"acme"
      """
    And the last relay subscription payload contains key fragment '{"tenant":"acme"}'
    And the last relay subscription payload does not contain "raw\""
    And the last relay subscription payload contains
      """
      "absolute":7
      "acos_value":1.04719
      "asin_value":0.52359
      "atan_value":1.10714
      "ceil_value":-1.0
      "cos_value":0.87758
      "exp_value":2.71828
      "floor_value":-2.0
      "ln_value":0.69314
      "log_value":2.0
      "log_base_value":6.64385
      "pow_value":8.0
      "round_value":2.0
      "sqrt_value":3.0
      "tan_value":0.54630
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |

  Scenario Outline: HTTP ingestor filter-map supports all Nervix internal schema types
    Given runtime replication is configured with replica count 0 and snapshot interval "100ms"
    And a <cluster_size> node nervix cluster is started
    And the leader node is configured with these NSPL commands
      """
      CREATE UNPACED DOMAIN {{domain}};
      """
    When the ingestor logic fixture "http_endpoint" starts with output schema "internal_types" and program
      """
      SET logic_notifications.u8_next = (message.u8 AS U8) + (1 AS U8),
          logic_notifications.i8_abs = abs(message.i8 AS I8),
          logic_notifications.u16_keep = coalesce((message.u16 AS U16), (0 AS U16)),
          logic_notifications.i16_prev = (message.i16 AS I16) - (1 AS I16),
          logic_notifications.u32_same = coalesce(nullif((message.u32 AS U32), (999 AS U32)), (0 AS U32)),
          logic_notifications.i32_neg = -(message.i32 AS I32),
          logic_notifications.u64_next = (message.u64 AS U64) + (2 AS U64),
          logic_notifications.i64_keep = message.i64,
          logic_notifications.f32_next = (message.f32 AS F32) + (1.5 AS F32),
          logic_notifications.f64_keep = message.f64,
          logic_notifications.bool_copy = message.active,
          logic_notifications.occurred_text = (message.occurred_at AS DATETIME) AS STRING,
          logic_notifications.occurred_copy = (message.occurred_at AS STRING) AS DATETIME
      UNSET logic_notifications.active, logic_notifications.u8, logic_notifications.i8, logic_notifications.u16, logic_notifications.i16, logic_notifications.u32, logic_notifications.i32, logic_notifications.u64, logic_notifications.i64, logic_notifications.f32, logic_notifications.f64, logic_notifications.occurred_at, logic_notifications.raw
      WHERE message.active AND (message.occurred_at AS DATETIME) > ('2026-04-07T00:00:00Z' AS DATETIME)
      """
    And the ingestor logic transport "http_endpoint" delivers payload fixture "internal_types_message"
    Then the relay subscription receives a payload
      """
      "tenant":"acme"
      """
    And the last relay subscription payload contains key fragment '{"tenant":"acme"}'
    And the last relay subscription payload does not contain "occurred_at\""
    And the last relay subscription payload contains
      """
      "u8_next":6
      "i8_abs":7
      "u16_keep":9
      "i16_prev":11
      "u32_same":42
      "i32_neg":11
      "u64_next":102
      "i64_keep":-64
      "f32_next":4.0
      "f64_keep":7.25
      "bool_copy":true
      "occurred_text":"2026-04-07T12:34:56+00:00"
      "occurred_copy":"2026-04-07T12:34:56+00:00"
      """

    Examples:
      | cluster_size |
      | 1            |
      | 3            |
