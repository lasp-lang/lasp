Definitions.

D   = [0-9]
L   = [A-Za-z]
WS  = ([\000-\s]|%.*)
C   = (<|<=|=|=>|>)

Rules.

where  : {token,{where,TokenLine,list_to_atom(TokenChars)}}.
or     : {token,{union,TokenLine,list_to_atom(TokenChars)}}.
and    : {token,{intersection,TokenLine,list_to_atom(TokenChars)}}.
{C}    : {token,{comparator,TokenLine,list_to_atom(TokenChars)}}.
'{L}+' : S = strip(TokenChars,TokenLen),
         {token,{string,TokenLine,S}}.
{L}+   : {token,{var,TokenLine,list_to_atom(TokenChars)}}.
{D}+   : {token,{integer,TokenLine,list_to_integer(TokenChars)}}.
{WS}+  : skip_token.

Erlang code.

strip(TokenChars,TokenLen) ->
    lists:sublist(TokenChars, 2, TokenLen - 2).

% Taken from http://blog.rusty.io/2011/02/08/leex-and-yecc/ and
% modified.
