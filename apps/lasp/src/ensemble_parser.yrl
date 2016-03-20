Nonterminals
statements statement expression int_list function.

Terminals
'<-' '+' '/' var integer nl.

Rootsymbol
statements.

statements -> statement : ['$1'].
statements -> statements nl statements : '$1' ++ '$3'.
statements -> statements nl : '$1'.

statement -> var '<-' expression : {update, '$1', '$3'}.
statement -> expression : '$1'.

expression -> var function expression : {map, '$1', '$2', '$3'}.
expression -> function '/' expression : {foldr, '$1', '$3'}.
expression -> var : {query, '$1'}.
expression -> int_list : '$1'.
expression -> integer : '$1'.

function -> '+' : {function, '$1'}.

int_list -> integer integer : ['$1', '$2'].
int_list -> integer int_list : ['$1'] ++ '$2'.
