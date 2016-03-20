Nonterminals
statements statement expression elements.

Terminals
'(' ')' ',' '<-' '+' var integer nl.

Rootsymbol
statements.

statements -> statement : ['$1'].
statements -> statements nl statements : '$1' ++ '$3'.
statements -> statements nl : '$1'.

statement -> var '<-' expression : {update, '$1', '$3'}.
statement -> expression : '$1'.

expression -> var : {query, '$1'}.
expression -> elements : '$1'.

elements -> integer : ['$1'].
elements -> integer elements : ['$1'] ++ '$2'.
