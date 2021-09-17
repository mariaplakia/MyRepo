-- Table: public.terms_df

-- DROP TABLE public.terms_df;

CREATE TABLE public.terms_df
(
    short_form text COLLATE pg_catalog."default",
    efo_label text COLLATE pg_catalog."default",
    iri text COLLATE pg_catalog."default",
    CONSTRAINT short_form UNIQUE (short_form)

)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.terms_df
    OWNER to postgres;_d