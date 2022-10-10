import numpy as np
import pandas as pd
import datetime as dt
from datetime import datetime

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func, inspect

from flask import Flask, jsonify

# create engine to hawaii.sqlite
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save references to each table
Measurement = Base.classes.measurement
Station=Base.classes.station

#################################################
# Flask Setup
#################################################
app = Flask(__name__)

#################################################
# Flask Routes
#################################################

@app.route("/")
def welcome():
    #List all available api routes.
    return (
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f'/api/v1.0/&lt;start date&gt<br/>'
        f'/api/v1.0/&lt;start date&gt;/&lt;end date&gt;'
    )

@app.route("/api/v1.0/precipitation")
def precipitation():
    # Precipitation analysis, retrieve only the last 12 months of data
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Find the most recent date in the data set.
    rows=session.query(func.max(Measurement.date)).first()
    recent_date=rows[0]

    # Starting from the most recent data point in the database. 
    recentdate=datetime.strptime(recent_date,"%Y-%m-%d")

    # Calculate the date one year from the last date in data set.
    year_ago = recentdate - dt.timedelta(days=366)

    # Perform a query to retrieve the data and precipitation scores
    sel = [Measurement.date, Measurement.prcp]
    prcp = session.query(*sel).filter(Measurement.date > year_ago).\
    filter(Measurement.date <= recentdate).all()
    session.close()

    date_list=[]
    prcp_list=[]
    for record in prcp:
        (mea_date, mea_prcp) = record
        if not mea_prcp is None:
            date_list.append(mea_date)
            prcp_list.append(mea_prcp)

    # To a dictionary using date as the key and prcp as the value
    dict_data=dict(zip(date_list,prcp_list))
    
    return jsonify(dict_data)

@app.route("/api/v1.0/stations")
def stations():
    # Return a list of stations
    # Create our session (link) from Python to the DB
    session = Session(engine)

    # stations
    stations_list=[]
    rows=session.query(Station.name).all()
    session.close()

    for record in rows:
        (station_name,)=record
        stations_list.append(station_name)

    return jsonify({"stations":stations_list})

@app.route("/api/v1.0/tobs")
def tobs():
    # Temperature observations of the most-active station for the previous year
    from sqlalchemy import desc

    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Find the most recent date in the data set.
    rows=session.query(func.max(Measurement.date)).first()
    recent_date=rows[0]

    # Starting from the most recent data point in the database. 
    recentdate=datetime.strptime(recent_date,"%Y-%m-%d")

    # Calculate the date one year from the last date in data set.
    year_ago = recentdate - dt.timedelta(days=366)

    sel = [Station.name, Station.station, func.count(Measurement.station)]
    rows=session.query(*sel).\
    filter(Station.station==Measurement.station).\
    group_by(Station.name).\
    order_by(desc(func.count(Measurement.station))).\
    all()

    (st_name, st_station, most_active) = rows[0]

    # Query the last 12 months of temperature observation data for this station and plot the results as a histogram
    df = pd.read_sql_query(
    sql = session.query(Measurement.date,Measurement.tobs).\
        filter(Measurement.date > year_ago).\
        filter(Measurement.date <= recentdate).\
        filter(Measurement.station == Station.station).\
        filter(Station.name == st_name).statement,
    con = engine
    )

    session.close()

    dict_data=df.to_dict('records')

    return jsonify({st_name:dict_data})

@app.route('/api/v1.0/<start>')
def stat_start(start):
    # Return a JSON list of the minimum temperature, the average temperature, and the maximum temperature for a specified start date
    # Create our session (link) from Python to the DB

    start_date=datetime.strptime(start,"%Y-%m-%d")

    session = Session(engine) 

    df = pd.read_sql_query(
    sql = session.query(
            func.min(Measurement.tobs).label("TMIN"), 
            func.avg(Measurement.tobs).label("TAVG"), 
            func.max(Measurement.tobs).label("TMAX")
            ).filter(Measurement.date >= start_date).statement,
    con = engine
    )
    session.close()

    dict_data=df.to_dict("records") 

    return jsonify(dict_data)


@app.route('/api/v1.0/<start>/<end>')
def stat_between(start,end):
    # Return a JSON list of the minimum temperature, the average temperature, and the maximum temperature for a specified start date
    # Create our session (link) from Python to the DB

    start_date=datetime.strptime(start,"%Y-%m-%d")
    end_date=datetime.strptime(end,"%Y-%m-%d")

    session = Session(engine) 

    df = pd.read_sql_query(
    sql = session.query(
            func.min(Measurement.tobs).label("TMIN"), 
            func.avg(Measurement.tobs).label("TAVG"), 
            func.max(Measurement.tobs).label("TMAX")
            ).\
            filter(Measurement.date >= start_date).\
            filter(Measurement.date <= end_date).\
            statement,
    con = engine
    )
    session.close()

    dict_data=df.to_dict("records") 

    return jsonify(dict_data)

if __name__ == "__main__":
    app.run(debug=True)