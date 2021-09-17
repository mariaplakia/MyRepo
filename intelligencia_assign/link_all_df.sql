-- Table: public.link_all_df

-- DROP TABLE public.link_all_df;

CREATE TABLE public.link_all_df
(
    child_short_form text COLLATE pg_catalog."default",
    parent_ontology_name text COLLATE pg_catalog."default",
    parent_short_form text COLLATE pg_catalog."default",
    parent_href text COLLATE pg_catalog."default",
    concatvalue text COLLATE pg_catalog."default",
    CONSTRAINT concatvalue2 UNIQUE (concatvalue)

)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.link_all_df
    OWNER to postgres;