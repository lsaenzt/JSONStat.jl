# JSONStat
A Tables.jl compliant for reading JSONStat files. Right now, only 'dataset' class is supported
Result is a JSONStat.Datatable that can be loaded into a DataFrame, saved with CSV or use any other Tables.jl-ready package

### Example
```julia
using Dataframes
HTTP.get("https://json-stat.org/samples/us-gsp.json").body |> JSONStat.read |> DataFrame
```

Additional information can be accesed using:
    - ```JSONStat.dimensions(dt::JSONStat.Datatable)```
    - ```JSONStat.metadata(dt::JSONStat.Datatable)```

Both functions return a Dictionary than can be pretty printed using ```JSONStat.pretty(d::Dict)```
