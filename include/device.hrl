-record(state, {
		  id, 
		  fixedhour, 
		  org_id,
		  kind,
		  settings, 
		  min_ptime,
		  sub_ev,
		  pad1,
		  history_raw,
		  history_events,
		  history_processed,
		  current_values,
		  usersub = [],
		  plugins_data = #{},
		  chour,
		  data,
		  last_ptime
		 }).
-record(incfg, {
		  dsname, 
		  type,
		  factor,
		  variable, 
		  limits,
		  ovfval,
		  multi,
		  pad1,
		  pad2
		 }).

