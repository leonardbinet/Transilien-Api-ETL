""" This module currently in progress will provide maps.
"""

import plotly.plotly as py
import plotly

# MAPBOX AUTHENTICATION
py.sign_in('leonardbinet', 'fR7JzzUPEZQn7ByEXtHh')
mapbox_access_token = "pk.eyJ1IjoibGVvbmFyZGJpbmV0IiwiYSI6ImNpdzBrNjU4NzAwMmwyb3BrYjQxemRoNnMifQ.7yzHGWbiQtCabkcgHa4oWw"

# Stations data
stations_data_path = "~/Documents/Formation/Cours_Telecom/T0_Projet_fil_rouge/api_transilien/data/gares_transilien_latlng.csv"
df_stat = pd.read_csv(stations_data_path, index_col="uic7")

peaks_series = all_stations_stops.T.max(axis=1)
df_stat["peak"] = peaks_series


scl = [
    [0.0, 'rgb(49,54,149)'],
    [0.1, 'rgb(69,117,180)'],
    [0.1, 'rgb(69,117,180)'],
    [0.2, 'rgb(116,173,209)'],
    [0.2, 'rgb(116,173,209)'],
    [0.3, 'rgb(171,217,233)'],
    [0.3, 'rgb(171,217,233)'],
    [0.4, 'rgb(224,243,248)'],
    [0.4, 'rgb(224,243,248)'],
    [0.5, 'rgb(254,224,144)'],
    [0.6, 'rgb(253,174,97)'],
    [0.7, 'rgb(253,174,97)'],
    [0.7, 'rgb(244,109,67)'],
    [0.8, 'rgb(215,48,39)'],
    [1, 'rgb(165,0,38)']
]

data = [dict(
    type='scattermapbox',
    # locationmode='USA-states',
    lon=df_stat['lng'],
    lat=df_stat['lat'],
    text=df_stat['Nom Gare'],
    mode='markers',
    marker=dict(
        size=8,
        opacity=0.8,
        reversescale=True,
        autocolorscale=False,
        symbol='circle',
        line=dict(
            width=1,
            color='rgba(102, 102, 102)'
        ),
        # colorscale=scl,
        cmin=0,
        color=df_stat["peak"],
        cmax=df_stat["peak"].max(),
        colorbar=dict(
            title="Max peak in day"
        )
    ))]

layout = dict(
    title='Station peaks)',
    width=1000,
    height=700,
    # autosize=True,
    hovermode='closest',
    colorbar=True,
    mapbox=dict(
        accesstoken=mapbox_access_token,
        bearing=0,
        center=dict(
            lat=48.823191,
            lon=2.579795,
        ),
        pitch=0,
        zoom=7
    ),
)
fig = dict(data=data, layout=layout)
# For ipython notebook
# py.iplot(fig, validate=False, filename='Stations peaks')
# For html
plotly.offline.plot(fig, validate=False)
