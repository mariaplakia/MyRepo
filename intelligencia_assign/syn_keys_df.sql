-- Table: public.syn_keys_df

-- DROP TABLE public.syn_keys_df;

CREATE TABLE public.syn_keys_df
(
    short_form text COLLATE pg_catalog."default",
    synonym_id bigint,
    concatvalue text COLLATE pg_catalog."default",
    CONSTRAINT concatvalue UNIQUE (concatvalue)

)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.syn_keys_df
    OWNER to postgres;