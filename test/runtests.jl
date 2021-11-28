using Test, HTTP
using JSONStat

resp = HTTP.get("https://json-stat.org/samples/canada.json").body

@test JSONStat.name(JSONStat.read(resp)) == "Population by sex and age group. Canada. 2012"
