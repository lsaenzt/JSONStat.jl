# JSONStat
A Tables.jl compliant JSONStat files reader. Only 'dataset' class is supported

```julia

HTTP.get("https://json-stat.org/samples/canada.json").body |> JSONStat.read

```
