# JSONStat
A Tables.jl compliant JSONStat files reader

'''julia

HTTP.get("https://json-stat.org/samples/canada.json").body |> JSONStat.read

'''
