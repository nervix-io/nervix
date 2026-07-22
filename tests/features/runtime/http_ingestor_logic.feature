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
      INHERIT ALL EXCEPT raw
      SET normalized = lower(trim(message.raw)), amount = message.amount + 1
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
      INHERIT ALL EXCEPT active, amount, raw
      SET amount_abs = abs(-input.amount),
          trimmed = trim(input.raw),
          lowered = lower(trim(input.raw)),
          uppered = upper(trim(input.raw)),
          raw_len = length(trim(input.raw)),
          contains_keep = contains(lower(trim(input.raw)), 'eep'),
          starts_keep = starts_with(lower(trim(input.raw)), 'keep'),
          ends_me = ends_with(lower(trim(input.raw)), 'me'),
          fallback = coalesce(nullif(lower(trim(input.raw)), 'keepme'), 'fallback'),
          was_keep = is_null(nullif(lower(trim(input.raw)), 'keepme'))
      WHERE input.active AND input.amount > 5 AND NOT starts_with(lower(trim(input.raw)), 'drop') AND (contains(lower(input.raw), 'keep') OR ends_with(lower(trim(input.raw)), 'me'))
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
      INHERIT ALL EXCEPT active, amount, raw
      SET parsed = input.raw AS INT64,
          sum = input.amount + (input.raw AS INT64),
          difference = input.amount - (input.raw AS INT64),
          product = input.amount * (input.raw AS INT64),
          quotient = input.amount / (input.raw AS INT64),
          remainder = input.amount % (input.raw AS INT64),
          complex = (input.amount + (input.raw AS INT64)) * (input.amount - (input.raw AS INT64)),
          comparison = (input.amount / (input.raw AS INT64)) > ((input.raw AS INT64) - 4),
          chained = lower(trim(upper(message.tenant)))
      WHERE (input.active AND input.amount < 0) OR (NOT input.active AND ((input.raw AS INT64) < input.amount AND input.amount > 10))
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
      INHERIT ALL EXCEPT active, amount, raw
      SET now_text = now() AS STRING,
          uuid4 = uuid_v4(),
          uuid7 = uuid_v7(),
          bit_len = bit_length(trim(input.raw)),
          ascii_value = ascii(trim(input.raw)),
          btrimmed = btrim(input.raw),
          char_len = char_length(input.raw),
          joined = concat('he', '-', message.tenant),
          titled = initcap(input.raw),
          lefted = left(trim(input.raw), 2),
          lowered = lower(trim(input.raw)),
          lpaded = lpad('he', 7, 'xy'),
          ltrimmed = ltrim(input.raw),
          digest = md5('he'),
          repeated = repeat('he', 2),
          replaced = replace(trim(input.raw), 'he', 'HE'),
          reversed = reverse('he'),
          righted = right(trim(input.raw), 2),
          rpaded = rpad('he', 7, 'xy'),
          rtrimmed = rtrim(input.raw),
          part = split_part('alpha.beta.gamma', '.', 2),
          starts = starts_with(lower(trim(input.raw)), 'he'),
          pos = strpos(trim(input.raw), 'he'),
          piece = substr(trim(input.raw), 2, 3),
          hexed = to_hex(255),
          translated = translate('he', 'he', 'HE'),
          trimmed2 = trim(input.raw),
          uppered = upper(message.tenant),
          regex_ok = regexp_like(trim(input.raw), 'h[a-z]+'),
          regex_replaced = regexp_replace(trim(input.raw), 'h[a-z]+', 'XX'),
          regex_piece = regexp_substr(input.raw, 'h[a-z]+')
      WHERE message.tenant = 'acme' AND input.active AND starts_with(lower(trim(input.raw)), 'he')
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
      INHERIT ALL EXCEPT active, amount, raw
      SET parsed = input.raw AS INT64,
          amount_text = input.amount AS STRING,
          amount_float = input.amount AS FLOAT64,
          truthy = 1 AS BOOLEAN,
          not_active = NOT input.active,
          literal_bool = true,
          literal_float = 3.5,
          literal_int = 9,
          label = 'ready',
          is_exact = input.raw = (input.amount AS STRING),
          negated = -input.amount
      WHERE message.tenant = 'acme' AND NOT input.active
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
      INHERIT ALL EXCEPT active, amount, raw
      SET absolute = abs(-input.amount),
          acos_value = acos(0.5),
          asin_value = asin(0.5),
          atan_value = atan(2.0),
          ceil_value = ceil(-1.75),
          cos_value = cos(0.5),
          exp_value = exp(1.0),
          floor_value = floor(-1.75),
          ln_value = ln(2.0),
          log_value = log(100.0),
          log_base_value = log(2.0, 100.0),
          pow_value = pow(2.0, 3.0),
          round_value = round(1.6),
          sqrt_value = sqrt(9.0),
          tan_value = tan(0.5)
      WHERE message.tenant = 'acme' AND input.active
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
      INHERIT ALL EXCEPT active, u8, i8, u16, i16, u32, i32, u64, i64, f32, f64, occurred_at, raw
      SET u8_next = (input.u8 AS U8) + (1 AS U8),
          i8_abs = abs(input.i8 AS I8),
          u16_keep = coalesce((input.u16 AS U16), (0 AS U16)),
          i16_prev = (input.i16 AS I16) - (1 AS I16),
          u32_same = coalesce(nullif((input.u32 AS U32), (999 AS U32)), (0 AS U32)),
          i32_neg = -(input.i32 AS I32),
          u64_next = (input.u64 AS U64) + (2 AS U64),
          i64_keep = input.i64,
          f32_next = (input.f32 AS F32) + (1.5 AS F32),
          f64_keep = input.f64,
          bool_copy = input.active,
          occurred_text = (input.occurred_at AS DATETIME) AS STRING,
          occurred_copy = (input.occurred_at AS STRING) AS DATETIME
      WHERE input.active AND (input.occurred_at AS DATETIME) > ('2026-04-07T00:00:00Z' AS DATETIME)
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
