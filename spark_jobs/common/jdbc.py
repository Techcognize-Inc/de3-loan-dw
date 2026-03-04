def jdbc_url():
    # Spark container talks to Postgres using docker service name "postgres"
    return "jdbc:postgresql://postgres:5432/bankingdb"

def jdbc_properties():
    return {
        "user": "postgres",
        "password": "password",
        "driver": "org.postgresql.Driver",
    }