from sqlalchemy import create_engine, text

engine = create_engine("postgresql+psycopg2://admin:admin@localhost:5433/testdb")

with engine.connect() as conn:
    result = conn.execute(text("SELECT 1"))
    for row in result:
        print(row)