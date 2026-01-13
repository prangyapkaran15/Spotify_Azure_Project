# Databricks notebook source
parameters = [
    {
        "table" : "spotify_catalog.silver.factstream",
        "alias" : "factstream",
        "columns": "factstream.stream_id , factstream.listen_duration"
        
    },
    {
       "table" : "spotify_catalog.silver.dimuser",
       "alias" : "dimuser",
       "columns": "dimuser.user_id, dimuser.user_name" ,
       "join": "factstream.user_id = dimuser.user_id"
    },
    {
       "table" : "spotify_catalog.silver.dimtrack",
       "alias" : "dimtrack",
       "columns": "dimtrack.track_id, dimtrack.track_name" ,
       "join": "factstream.track_id = dimtrack.track_id"
    }

]

# COMMAND ----------

pip install jinja2

# COMMAND ----------

Query_text = """
        SELECT
{% for param in parameters %}
    {{ param.columns }}{% if not loop.last %},{% endif %}
{% endfor %}

FROM
{% for param in parameters %}
    {% if loop.first %}
        {{ param.table }} AS {{ param.alias }}
    {% else %}
        LEFT JOIN {{ param.table }} AS {{ param.alias }}
        ON {{ param.join }}
    {% endif %}
{% endfor %}


"""

# COMMAND ----------

from jinja2 import Template
jinja_sql_query = Template(Query_text).render(parameters=parameters)
print(jinja_sql_query)

# COMMAND ----------

display(spark.sql(jinja_sql_query))
