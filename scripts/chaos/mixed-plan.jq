# Validates a mixed-instability plan and prints the list of reasons it breaks its contract, empty
# for a valid plan. Read by mixed-plan.sh validate, which passes $overheads, the seconds a step of
# each kind is planned to take beyond its longest hold.

def node_refs: ["target", "peer", "other"];
def network: .family == "partition" or .family == "degrade";
def nodes_of: if .node == "cluster" then node_refs else [.node] end;

# The references that may lead while a step runs: a leader step's target leads, and a follower
# step's target does not, so its peer does. An owner step's target may lead or not.
def possible_leaders:
  if .role == "leader" then ["target"]
  elif .role == "follower" then ["peer"]
  else ["target", "peer"] end;

# The voters an action leaves out of a working quorum while $leader leads. One-way loss excludes
# its receiver, or its sender when the receiver leads, because the leader keeps leading; a degraded
# link excludes nobody.
def impaired($leader):
  if .family == "partition" then
    (if .partition == "quorum-loss" then node_refs
     elif .partition == "one-way" then (if .to == $leader then [.node] else [.to] end)
     else [.node] end)
  elif .family == "degrade" then []
  else nodes_of end;

# The fields an action of its family takes.
def allowed_fields:
  ["id", "family", "node", "start_seconds", "hold_seconds"]
  + (if .family == "partition" then ["partition"] + (if .partition == "one-way" then ["to"] else [] end)
     elif .family == "degrade" then ["to", "profile"]
     else [] end);

# The nodes whose interfaces carry a network action's rules: an isolation's peers, the sender of a
# one-way or degraded link, or every node in quorum loss.
def rules_on:
  if .family == "partition" then
    (if .partition == "isolate" then node_refs - [.node]
     elif .partition == "one-way" then [.node]
     else node_refs end)
  elif .family == "degrade" then [.node]
  else [] end;

def hold_bounds:
  {kill: [5, 120], stop: [5, 120], pause: [1, 99], partition: [20, 600], degrade: [10, 300]}[.family];

def whole: type == "number" and isfinite and floor == .;

def coverage_item:
  test("^((kill|stop|pause|partition|degrade):(leader|follower|ingestor-owner|relay-owner|emitter-owner|cluster)|quorum-loss)$");

def action_reasons($step):
  . as $action
  | "action \($action.id)" as $label
  | if ($action.id | type == "string" and test("^[0-9a-z]+$")) | not then
      "an action of step \($step.index) needs an id of lowercase letters and digits, which names its containers and canaries"
    elif (["kill", "stop", "pause", "partition", "degrade"] | index($action.family)) == null then
      "\($label) has unknown family \($action.family)"
    elif ($action.start_seconds | whole and . >= 0) | not then
      "\($label) needs a whole start_seconds of 0 or more"
    elif ($action.hold_seconds | whole) | not then
      "\($label) needs a whole hold_seconds"
    elif $action.hold_seconds < ($action | hold_bounds)[0] or $action.hold_seconds > ($action | hold_bounds)[1] then
      "\($label) holds \($action.family) for \($action.hold_seconds) s, outside \(($action | hold_bounds)[0])..\(($action | hold_bounds)[1]) s"
    elif ((node_refs + ["cluster"]) | index($action.node)) == null then
      "\($label) names unknown node \($action.node)"
    elif ($step.role == "cluster") != ($action.node == "cluster") then
      "\($label) names \($action.node) in a step whose role is \($step.role); only cluster steps name the cluster"
    elif $action.family == "partition" and (["isolate", "one-way", "quorum-loss"] | index($action.partition)) == null then
      "\($label) has unknown partition \($action.partition)"
    elif $action.node == "cluster"
         and (($action.family == "kill" or ($action.family == "partition" and $action.partition == "quorum-loss")) | not) then
      "\($label) applies \($action.family) to the whole cluster; only kill and a quorum-loss partition do"
    elif $action.family == "partition" and $action.partition == "quorum-loss" and $action.node != "cluster" then
      "\($label) is a quorum-loss partition and must name the cluster"
    elif ($action.family == "degrade" or $action.partition == "one-way")
         and ((node_refs | index($action.to)) == null or $action.to == $action.node) then
      "\($label) needs a link to another node of the step"
    elif (($action | keys) - ($action | allowed_fields) | length) > 0 then
      "\($label) has \(($action | keys) - ($action | allowed_fields) | join(", ")), which its family does not take"
    elif $action.family == "degrade"
         and (["delay", "jitter", "random-loss", "burst-loss", "rate-limit", "combined"] | index($action.profile)) == null then
      "\($label) has unknown degradation profile \($action.profile)"
    else empty end;

def step_reasons($offset):
  . as $step
  | if type != "object" then "step \($offset + 1) is not an object"
    else
      (if $step.index != $offset + 1 then
         "step \($offset + 1) carries index \($step.index); steps are numbered from 1 in order" else empty end),
      (if (["kill", "stop", "pause", "isolate", "one-way", "degrade", "outage-under-degrade", "isolated-restart",
            "quorum-loss", "double-outage", "cluster-restart"] | index($step.kind)) == null then
         "step \($step.index) has unknown kind \($step.kind)" else empty end),
      (if (["leader", "follower", "ingestor-owner", "relay-owner", "emitter-owner", "cluster"] | index($step.role)) == null then
         "step \($step.index) has unknown role \($step.role)" else empty end),
      (if ($step.pick | whole and . >= 0) | not then "step \($step.index) needs a whole pick of 0 or more" else empty end),
      (if ($step.at_seconds | whole and . >= 0) | not then "step \($step.index) needs a whole at_seconds of 0 or more" else empty end),
      (if ($step.estimated_seconds | whole and . >= 1) | not then
         "step \($step.index) needs a positive whole estimated_seconds" else empty end),
      (if ($step.actions | type == "array" and length > 0) | not then "step \($step.index) holds no actions"
       else
         ($step.actions[]
          | if type != "object" then "step \($step.index) holds an action that is not an object"
            else action_reasons($step) end)
       end)
    end;

# Sweeps one well-formed step's faults in planned order while $leader leads. Heals come first at
# equal offsets, so a fault healed at the instant another is injected never overlaps it. The result
# holds the reasons the step breaks its contract, whether two voters are ever out of a quorum at
# once, and the longest such interval.
def sweep($policy; $leader):
  . as $step
  | [$step.actions[] | {t: .start_seconds, order: 1, a: .}, {t: (.start_seconds + .hold_seconds), order: 0, a: .}]
  | sort_by(.t, .order)
  | reduce .[] as $event ({active: [], errors: [], since: null, longest: 0, lost: false};
      $event.a as $a
      | if $event.order == 1 then
          .errors += [
            (.active[] | select(($a | network) and network)
             | "step \($step.index): conflicting interface rules: \($a.id) would be installed while \(.id) holds its own network rules"),
            (.active[] | select(($a | network) and (network | not))
             | "step \($step.index): network fault \($a.id) would be installed while node fault \(.id) holds a node out of the link checks"),
            (.active[] | select(($a | network | not) and network)
             | select([($a | nodes_of)[] as $node | rules_on | index($node)] | any(. != null))
             | "step \($step.index): conflicting interface rules: node fault \($a.id) targets a node that carries the rules of \(.id)"),
            (.active[] | select(($a | network | not) and (network | not))
             | select([($a | nodes_of)[] as $node | nodes_of | index($node)] | any(. != null))
             | "step \($step.index): node fault \($a.id) targets a node that \(.id) already holds out")
          ]
          | .active += [$a]
          | ([.active[] | impaired($leader)[]] | unique | length) as $count
          | (if $count >= 2 and $policy == "preserve-quorum" then
               .errors += ["step \($step.index): \($a.id) would leave \($count) of 3 voters out of a quorum, which preserve-quorum refuses"]
             else . end)
          | (if $count >= 2 and .since == null then .since = $event.t | .lost = true else . end)
        else
          .errors += [
            (.active[] | select(($a | network) and (network | not))
             | "step \($step.index): network fault \($a.id) would be healed while node fault \(.id) holds a node out of the link checks")
          ]
          | .active |= map(select(.id != $a.id))
          | ([.active[] | impaired($leader)[]] | unique | length) as $count
          | (if $count < 2 and .since != null then
               .longest = ([.longest, $event.t - .since] | max) | .since = null
             else . end)
        end)
  | {errors, longest, lost};

# The reasons one well-formed step breaks its contract, judged for every reference that may lead it.
def step_contract_reasons($policy):
  . as $step
  | [possible_leaders[] as $leader
     | $step | sweep($policy; $leader)
     | .errors[],
       (if .longest > 120 then
          "step \($step.index) loses quorum for \(.longest) s; a temporary quorum loss lasts at most 120 s" else empty end)]
  | unique[];

# Whether a well-formed step leaves two voters out of a quorum at once whichever reference leads it.
def loses_quorum($policy):
  . as $step
  | [possible_leaders[] as $leader | $step | sweep($policy; $leader) | .lost] | all;

. as $plan
| [
    (if (["preserve-quorum", "temporary-quorum-loss"] | index($plan.policy)) == null then
       "the policy must be preserve-quorum or temporary-quorum-loss" else empty end),
    (if $plan.nodes != 3 then "a mixed plan runs on exactly three nodes" else empty end),
    (if ($plan.seed == null or ($plan.seed | whole and . >= 0 and . <= 2147483646)) | not then
       "the seed must be null or an integer from 0 through 2147483646" else empty end),
    (if ($plan.duration_seconds | whole and . >= 1) | not then "duration_seconds must be a positive whole number" else empty end),
    (if ($plan.coverage | type == "array" and length > 0 and all(.[]; type == "string")) | not then
       "coverage must be a non-empty list of items" else empty end),
    (if ($plan.steps | type == "array" and length > 0) | not then "the plan must hold at least one step" else empty end)
  ] as $shape
| if ($shape | length) > 0 then $shape
  else
    [ ($plan.coverage[] | select(coverage_item | not) | "unknown coverage item \(.)"),
      ($plan.coverage[] | select(coverage_item) | select(. == "quorum-loss" or endswith(":cluster"))
       | select($plan.policy != "temporary-quorum-loss")
       | "coverage item \(.) loses quorum and needs the temporary-quorum-loss policy"),
      ($plan.steps | to_entries[] | .key as $offset | .value | step_reasons($offset))
    ] as $structure
  | if ($structure | length) > 0 then $structure
    else
      [ ([$plan.steps[].actions[].id] | group_by(.) | map(select(length > 1) | .[0]) | .[]
         | "action id \(.) is used more than once"),
        ($plan.steps[] | step_contract_reasons($plan.policy)),
        # A run sizes its fixture from the plan's end, so no step may be planned shorter than the
        # generator plans a step of its kind.
        ($plan.steps[] | ([.actions[] | .start_seconds + .hold_seconds] | max) as $longest
         | ($longest + $overheads[.kind]) as $needed
         | select(.estimated_seconds < $needed)
         | "step \(.index) is estimated at \(.estimated_seconds) s; its faults hold until \($longest) s and a step of kind \(.kind) needs \($overheads[.kind]) s beyond its holds, so at least \($needed) s"),
        ($plan.steps | to_entries[] | select(.key > 0)
         | . as $entry | $plan.steps[$entry.key - 1] as $previous
         | select($entry.value.at_seconds < $previous.at_seconds + $previous.estimated_seconds)
         | "step \($entry.value.index) starts at \($entry.value.at_seconds) s, before step \($previous.index) is planned to end"),
        ($plan.steps[-1] | select(.at_seconds + .estimated_seconds > $plan.duration_seconds)
         | "the plan ends at \(.at_seconds + .estimated_seconds) s, after its \($plan.duration_seconds)-second duration"),
        # A coverage item is planned when an action of its family targets the step's role, or when a
        # step leaves two voters out of a quorum at once whichever reference leads it.
        (([$plan.steps[] | .role as $role | .actions[]
           | select(.node == "target" or .node == "cluster") | "\(.family):\($role)"]
          + [$plan.steps[] | select(loses_quorum($plan.policy)) | "quorum-loss"])
         | unique) as $planned
        | ($plan.coverage[] | . as $item | select(($planned | index($item)) == null)
           | "coverage item \($item) is required but no action of the plan covers it")
      ]
    end
  end
