Nonterminals
predicates predicate element.

Terminals atom var integer string union intersection comparator.

Expect 1.

Rootsymbol predicates.

predicates -> predicate : '$1'.
predicates -> predicate union predicate : {union, '$1', '$3'}.
predicates -> predicates union predicate : {union, '$1', '$3'}.

predicates -> predicate intersection predicate : {intersection, '$1', '$3'}.

predicate -> var comparator element : {predicate, {var, unwrap('$1')}, unwrap('$2'), '$3'}.

element -> atom : '$1'.
element -> var : unwrap('$1').
element -> integer : unwrap('$1').
element -> string : unwrap('$1').

Erlang code.

unwrap({_,_,V}) -> V.
