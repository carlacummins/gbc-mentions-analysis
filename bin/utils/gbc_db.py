#!/usr/bin/env python3

from google.cloud.sql.connector import Connector
import pymysql
import sqlalchemy as db


def get_gbc_connection(test=False, readonly=True, sqluser="gbcreader", sqlpass=None):
    if not readonly and not sqlpass:
        raise ValueError("You must provide a SQL user credentials if not in readonly mode.")

    database = "gbc-publication-analysis:europe-west2:gbc-sql/gbc-publication-analysis"
    database += "-test" if test else ""
    instance, db_name = database.split('/')

    gcp_connector = Connector()
    def getcloudconn() -> pymysql.connections.Connection:
        conn: pymysql.connections.Connection = gcp_connector.connect(
            instance, "pymysql",
            user=sqluser,
            password=sqlpass,
            db=db_name
        )
        return conn

    cloud_engine = db.create_engine("mysql+pymysql://", creator=getcloudconn, pool_recycle=60 * 5, pool_pre_ping=True)
    return (gcp_connector, cloud_engine, cloud_engine.connect())