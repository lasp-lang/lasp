-define(
INPUT_DATA,
    [
        [{"id1", "a b c d b"}, {"id2", "b c a c"}, {"id3", "d e f g h"},
            {"id4", "a f t e f"}, {"id5", "w q q r t t d"},
            {"id6", "d f v b w"}, {"id7", "f j k l k j"}],
        [{"id8", "l p k i k"}, {"id9", "h i h f"}, {"id10", "a d f s f"},
            {"id11", "h k l j k k k"}, {"id12", "a s d f g d s"},
            {"id13", "k h k j l l h"}, {"id14", "a d f s d d f f"}],
        [{"id15", "q r t f d f r"}, {"id16", "i o u y"}, {"id17", "o t g b n"},
            {"id18", "z x c v c x"}, {"id19", "n x c w r"},
            {"id20", "z c e r f"}, {"id21", "k r f g s s d"}]]).

%% Input set id.
-define(SET_DOC_ID_TO_CONTENTS, {<<"doc_id_to_contents">>, ps_aworset}).

%% Intermediate set ids.
-define(SET_DOC_ID_TO_WORDS_NO_DUP, {<<"doc_id_to_words_no_dup">>, ps_aworset}).
-define(SET_WORD_TO_DOC_IDS, {<<"word_to_doc_ids">>, ps_singleton_orset}).
-define(SET_WORD_TO_DOC_COUNTS, {<<"word_to_doc_counts">>, ps_singleton_orset}).
-define(SET_WORD_TO_DOC_COUNT_LIST, {<<"word_to_doc_count_list">>, ps_aworset}).
-define(SET_WORD_TO_DOC_COUNT, {<<"word_to_doc_count">>, ps_aworset}).
-define(
SIZE_T_DOC_ID_TO_CONTENTS, {<<"size_t_doc_id_to_contents">>, ps_size_t}).
-define(
SET_WORD_TO_DOC_COUNT_AND_NUM_OF_DOCS,
    {<<"word_to_doc_count_and_num_of_docs">>, ps_aworset}).

%% Result set id.
-define(SET_WORD_TO_DOC_FREQUENCY, {<<"word_to_doc_frequency">>, ps_aworset}).

%% A counter for completed clients.
-define(
    COUNTER_COMPLETED_CLIENTS, {<<"counter_completed_clients">>, ps_gcounter}).
