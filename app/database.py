import json
import logging

import psycopg2


class DatabaseClient:

    # TODO Store table names as constants

    def __init__(self, host: str, port: int, user: str, password: str, db_name: str):
        self.connection = psycopg2.connect("host='{}' port={} user='{}' password='{}' dbname='{}'".format(
            host, port, user, password, db_name
        ))
        self.init_db()

    def commit(self):
        self.connection.commit()

    def execute(self, query: str, params: tuple = None, commit: bool = False):
        self.connection.cursor().execute(query, params)
        if commit:
            self.commit()

    def init_db(self):
        self.execute(commit=True, query="""
        DROP TABLE IF EXISTS public.article_performance;
        CREATE TABLE public.article_performance (
            article_id varchar NOT NULL,
            "date" timestamp NOT NULL,
            title varchar NOT NULL,
            category varchar NOT NULL,
            card_views int8 NOT NULL DEFAULT 0,
            article_views int8 NOT NULL DEFAULT 0,
            CONSTRAINT article_performance_pk PRIMARY KEY (article_id, date)
        );
        DROP TABLE IF EXISTS public.stg_article;
        CREATE TABLE public.stg_article (
            article_id varchar NOT NULL,
            title varchar NULL,
            category varchar NULL,
            CONSTRAINT stg_table_pk PRIMARY KEY (article_id)
        );
        DROP TABLE IF EXISTS public.stg_article_performance;
        CREATE TABLE public.stg_article_performance (
            article_id varchar NOT NULL,
            "date" date NOT NULL,
            card_views int8 NOT NULL DEFAULT 0,
            article_views int8 NOT NULL DEFAULT 0
        );
        DROP TABLE IF EXISTS public.stg_events;
        CREATE TABLE public.stg_events (
            "timestamp" timestamptz NOT NULL,
            "session_id_md5" char(32) NOT NULL,
            "event_name" varchar(255) NULL,
            "user_id_md5" char(32) NOT NULL,
            "attributes" jsonb NOT NULL DEFAULT '{}'
        );
        DROP TABLE IF EXISTS public.stg_user_performance;
        CREATE TABLE public.stg_user_performance (
            user_id varchar NOT NULL,
            "date" date NOT NULL,
            ctr float8 NULL
        );
        DROP TABLE IF EXISTS public.user_performance;
        CREATE TABLE public.user_performance (
            article_id varchar NOT NULL,
            "date" timestamp NOT NULL,
            ctr float8 NOT NULL DEFAULT 0.0
        );
        """)

    def insert_event(self, timestamp, session_id_md5, event_name, user_id_md5, attributes):
        params = (timestamp, session_id_md5, event_name, user_id_md5, json.dumps(attributes))
        self.execute("""
            INSERT INTO public.stg_events ("timestamp", session_id_md5, event_name, user_id_md5, "attributes") 
            VALUES(%s, %s, %s, %s, %s);
        """, params)

    def build_staging_tables(self):
        logging.info("Building staging table (1 of 3): stg_article")
        self.execute("""
            insert into stg_article 
            select distinct on ("attributes"->>'id')
                "attributes"->>'id' as article_id,
                last_value("attributes"->>'title') over (partition by "attributes"->>'id' order by "timestamp") as title,
                last_value("attributes"->>'category') over (partition by "attributes"->>'id' order by "timestamp") as category
            from stg_events se
            where "attributes"->>'id' is not null 
              and "attributes"->>'title' is not null
              and "attributes"->>'category' is not null;
        """)
        logging.info("Building staging table (2 of 3): stg_article_performance")
        self.execute("""
            insert into stg_article_performance
            select
                "attributes"->>'id' as article_id,
                date_trunc('day', "timestamp") as "date",
                count(*) FILTER (WHERE event_name = 'article_viewed') as card_views,
                count(*) FILTER (WHERE event_name in ('top_news_card_viewed', 'my_news_card_viewed')) as article_views
            from
                stg_events e
            where
                event_name in ('article_viewed', 'top_news_card_viewed', 'my_news_card_viewed')
                and "attributes" ? 'id'
            group by 1, 2;
        """)
        logging.info("Building staging table (3 of 3): stg_user_performance")
        self.execute("""
            insert into stg_user_performance 
            select 
                user_id_md5 as user_id,
                date_trunc('day', "timestamp") as "date",
                coalesce(
                    (count(*) FILTER (WHERE event_name = 'article_viewed'))::decimal / 
                    nullif(count(*) FILTER (WHERE event_name in ('top_news_card_viewed', 'my_news_card_viewed')), 0)
                , 0) as ctr
            from 
                stg_events e 
            where 
                e.user_id_md5 is not null 
            group by 1, 2;
        """)
        self.commit()

    def build_final_tables(self):
        logging.info("Building final table (1 of 2): article_performance (from: stg_article, stg_article_performance) ")
        self.execute("""
            insert into article_performance
            select
                sap.article_id, 
                sap."date", 
                sa.title, 
                sa.category, 
                sap.card_views, 
                sap.article_views
            from 
                stg_article_performance sap 
            join stg_article sa on (sa.article_id = sap.article_id )
        """)
        logging.info("Building final table (2 of 2): user_performance (from: stg_user_performance)")
        self.execute("""
            insert into user_performance
            select *
            from
                stg_user_performance
        """)
        self.commit()
