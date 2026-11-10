# Read the current public StateInspection envelope for the chaos profile schemas. A page that
# omits anything cannot establish either a profile version or the absence of a branch.
def require($condition; $message): if $condition then . else error($message) end;
def count: type == "number" and . >= 0 and floor == .;
def counts: type == "object" and (.branches | count) and (.rows | count) and (.bytes | count);
def fields:
    type == "array" and length > 0
    and all(.[]; (.name | type == "string" and length > 0)
        and (.ty == "String" or .ty == "I64" or .ty == "Datetime")
        and .optional == false and .sensitive == false)
    and (map(.name) | unique | length) == length;
def cells($fields):
    . as $cells
    | require(type == "array" and length == ($fields | length); "row/field count mismatch")
    | [range(0; $fields | length) as $index
       | $fields[$index] as $field | $cells[$index] as $cell
       | require(($cell | type) == "object" and ($cell | has("value"))
           and $cell.type == ($field.ty | ascii_downcase)
           and (if $field.ty == "String" then ($cell.value | type) == "string"
                else ($cell.value | type) == "number" and ($cell.value | floor) == $cell.value end);
           "profile cell does not conform to its schema")
       | {key: $field.name, value: $cell.value}]
    | from_entries;
def section_rows:
    require(.name == "records" and .omitted_rows == 0; "missing or incomplete records section")
    | require((.schema.fields | fields) and (.schema.branch.fields | fields)
        and (.schema.branch.name | type == "string" and length > 0)
        and (.schema.branch.fields | map({name, ty})) == [{name:"branch_name", ty:"String"}]
        and any(.schema.fields[]; .name == "branch_name" and .ty == "String")
        and any(.schema.fields[]; .name == "profile_version" and .ty == "I64")
        and (.watermarks.low | count) and (.watermarks.high | count)
        and .watermarks.low != .watermarks.high
        and .schema.fields[.watermarks.low].ty == "Datetime"
        and .schema.fields[.watermarks.high].ty == "Datetime";
        "invalid profile schema or watermark columns")
    | . as $section
    | require(.frames | type == "array"; "missing row frames")
    | .frames[]
    | (.branch_key | cells($section.schema.branch.fields)) as $key
    | require(.rows | type == "array" and length == 1; "a materialized branch must hold one row")
    | .rows[] | cells($section.schema.fields) | {key: $key, payload: .};
def unit_rows:
    require((.counts | counts) and (.sections | type == "array" and length <= 1);
        "missing unit counts or sections")
    | . as $unit
    | [.sections[] | section_rows] as $rows
    | require(($rows | length) == $unit.counts.rows
        and ($rows | map(.key) | unique | length) == $unit.counts.branches;
        "incomplete unit observation")
    | $rows;

require(length == 1; "expected one inspection document") | .[0]
| require(.entity == {domain: "chaos_baseline", kind: "Relay", name: $relay}; "wrong inspected entity")
| require(.outcome == "page"; "inspection did not return a page")
| require(has("continuation") and .continuation == null; "inspection page has unread state")
| require((.counts | counts) and (.unit_count | count and . > 0)
    and (.units | type == "array") and (.units | length) == .unit_count;
    "missing counts or units")
| require((.units | map(.unit) | unique | length) == .unit_count
    and (if .units[0].unit == "Single" then .unit_count == 1 else
        (.units | map(.unit.Partition) | sort) == [range(0; .unit_count)] end);
    "invalid or repeated execution units")
| [.units[] | unit_rows | .[]] as $rows
| require(($rows | length) == .counts.rows
    and ($rows | map(.key) | unique | length) == .counts.branches
    and ([.units[].counts.bytes] | add) == .counts.bytes;
    "incomplete page observation")
| $rows
