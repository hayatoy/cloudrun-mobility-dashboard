import threading
import time
import streamlit as st
import altair as alt
from vega_datasets import data
from queue import Queue
from google.cloud import firestore
from streamlit.components.v1 import html
import pandas as pd
import numpy as np
import pydeck as pdk
import logging

db = firestore.Client()
q = Queue()

chart_data = pd.DataFrame(
  np.random.randn(20, 3),
  columns=['a', 'b', 'c'])

st.title('Firestore Demo')
st.subheader('Current Locations')

# create blank DataFrame
df_trip = pd.DataFrame(columns=['path', 'timestamps'], data=None)


trip_layer = pdk.Layer(
  'TripsLayer',
  df_trip,
  get_path="path",
  get_timestamps="timestamps",
  get_color=[253, 128, 93],
  opacity=1,
  width_min_pixels=3,
  rounded=True,
  trail_length=8,
  current_time=100,
)

r = pdk.Deck(
  map_provider="carto",
  map_style='road',
  initial_view_state=pdk.ViewState(
    height=380,
    latitude=40.72,
    longitude=-74,
    zoom=13,
  #  tilt=45
    pitch=50,
  ),
  layers=[trip_layer],
)
rt_map = st.pydeck_chart(r)

col1, col2 = st.columns([3, 1])
col1.subheader('Dummy Chart')
col1.line_chart(chart_data)
col2.subheader('Snapshot:')
with col2:
  snap = st.empty()

# Create a callback on_snapshot function to capture changes
def on_snapshot(col_snapshot, changes, read_time):

  doc_list = []
  for change in changes:
    doc = change.document.to_dict()
    doc['id'] = str(change.document.id)
    doc_list.append(doc)
  q.put(doc_list)

def main():
  col_ref = db.collection('connected')

  # Watch the document
  col_watch = col_ref.on_snapshot(on_snapshot)

  while True:
    doc_list = q.get()

    if doc_list[0]['timestamp'] == 0:
      df_trip = pd.DataFrame(columns=['path', 'timestamps'], data=None)

    for doc in doc_list:
      vehicle_ind = doc['id']

      try:
        _path = df_trip.at[vehicle_ind, 'path']
      except KeyError:
        _path = []
      _path.append(doc['lonlat'])
      df_trip.at[vehicle_ind, 'path'] = _path

      _timestamps = df_trip.at[vehicle_ind, 'timestamps']
      if type(_timestamps) != list:
        _timestamps = []
      _timestamps.append(doc['timestamp'])
      df_trip.at[vehicle_ind, 'timestamps'] = _timestamps
    
    snap.write(doc_list)
    trip_layer.current_time = doc_list[-1]['timestamp']
    trip_layer.data = df_trip
    r.update()
    rt_map.pydeck_chart(r)


if __name__ == "__main__":
  main()