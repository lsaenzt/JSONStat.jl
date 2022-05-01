using Test, Downloads
using JSONStat

io = IOBuffer()
resp = Downloads.download("https://json-stat.org/samples/canada.json", io) |> take!

@test JSONStat.name(JSONStat.read(resp)) == "Population by sex and age group. Canada. 2012"


resp = Downloads.download("https://json-stat.org/samples/galicia.json", io) |> take!
@test JSONStat.Tables.columnnames(galicia) |> length == 7

resp = Downloads.download("https://json-stat.org/samples/us-gsp.json", io) |> take!
