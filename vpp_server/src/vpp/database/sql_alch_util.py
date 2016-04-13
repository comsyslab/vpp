import sqlalchemy


def create_engine_and_session(db_string, autoflush=True):
    engine = sqlalchemy.create_engine(db_string, echo=False)
    SessionCls = sqlalchemy.orm.sessionmaker(bind=engine)
    session = SessionCls()
    session.autoflush = autoflush

    return engine, session
