## DATA EXPLORATION

These scripts are not part of the application, these are just explorary 
scripts to explore generated data during multiple part of the process.
 
 It includes:
 - a script to explore **"initial_exploration"**, these are raw stoptimes with delay information 
 when we managed to map it with API data (data obtained using schedule and realtime querier modules).
 - a script to explore feature matrices and compute **"delay_prediction"**, these are matrices built to provide
 training sets for ML algorithms (obtained through feature_vector_builder module).
 - a script to analyse our **"prediction_score"**: to see how well our predictors are accurate. Not implemented yet.
 - a script **"line_dataviz_parser"** made to transform data in a suitable format for data-visualization.