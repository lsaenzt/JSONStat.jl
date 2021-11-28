using Test, HTTP
using JSONStat

resp = HTTP.get("https://servicios.ine.es/wstempus/jsstat/ES/DATASET/22344?date=20210601").body

@test JSONStat.read(resp).name == "Índices nacionales: general y de grupos ECOICOP"