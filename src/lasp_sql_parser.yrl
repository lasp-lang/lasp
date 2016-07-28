Nonterminals
statement select_clause from_clause where_clause predicates predicate element.

Terminals atom var integer string select from where union intersection comparator.

Expect 1.

Rootsymbol statement.

statement -> select_clause from_clause where_clause : {query, '$1', '$2', '$3'}.

select_clause -> select element : {select, '$2'}.

from_clause -> from element : {from, '$2'}.

where_clause -> where predicates : {where, '$2'}.

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
