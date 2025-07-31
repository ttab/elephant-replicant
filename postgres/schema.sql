--
-- PostgreSQL database dump
--

-- Dumped from database version 17.4 (Debian 17.4-1.pgdg120+2)
-- Dumped by pg_dump version 17.4 (Debian 17.4-1.pgdg120+2)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: document; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.document (
    id uuid NOT NULL,
    target_version bigint NOT NULL
);


--
-- Name: schema_version; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.schema_version (
    version integer NOT NULL
);


--
-- Name: state; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.state (
    name text NOT NULL,
    value jsonb NOT NULL
);


--
-- Name: version_mapping; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.version_mapping (
    id uuid NOT NULL,
    source_version bigint NOT NULL,
    target_version bigint NOT NULL,
    created timestamp with time zone NOT NULL
);


--
-- Name: document document_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.document
    ADD CONSTRAINT document_pkey PRIMARY KEY (id);


--
-- Name: state state_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.state
    ADD CONSTRAINT state_pkey PRIMARY KEY (name);


--
-- Name: version_mapping version_mapping_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.version_mapping
    ADD CONSTRAINT version_mapping_pkey PRIMARY KEY (id, source_version);


--
-- Name: idx_mapping_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_mapping_created ON public.version_mapping USING btree (created);


--
-- PostgreSQL database dump complete
--

