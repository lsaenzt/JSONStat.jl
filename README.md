# JSONStat
A Tables.jl compliant JSONStat files reader. Only 'dataset' class is supported

```julia

HTTP.get("https://json-stat.org/samples/canada.json").body |> JSONStat.read

```
Result is a JSONStat.Datatable that can be loaded into a DataFrame, saved with CSV or use any other Tables.jl ready package

Additional information can be accesed using:
    - JSONStat.dimensions(dt::JSONStat)
    - JSONStat.metadata(dt::JSONStat)