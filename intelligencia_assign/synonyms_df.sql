-- Table: public.synonyms_df

-- DROP TABLE public.synonyms_df;

CREATE TABLE public.synonyms_df
(
    synonym_id bigint,
    synonyms_label text COLLATE pg_catalog."default",
    CONSTRAINT synonym_id UNIQUE (synonym_id)

)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.synonyms_df
    OWNER to postgres;