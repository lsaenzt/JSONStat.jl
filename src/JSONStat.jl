module JSONStat

# TODO: add Units and Child to dimensions
# TODO: Deal with 'Collection' class
# TODO: status field

using JSON3, Tables, PrettyTables, OrderedCollections

# Field to include in dataset metadata and dimension data
const metadatafields = (:updated, :note, :extension, :href, :type, :link, :role, :error) # excluding :version, :class
const dimfields = (:note,:href,:label)
const categoryfields = (:child, :coordinates, :unit)

# Wrapper for controlling dispatching (read is a very common name for a function)
struct JSONStatData
    js::JSON3.Object
    JSONStatData(js) = haskey(js,:version) ? new(js) : error("JSON data does not seem to conform JSONStat 2.0 standard")
end

# Output struct for defining specific show method and accessors
struct Datatable <: Tables.AbstractColumns
    name::String
    data::NamedTuple
    source::String
    dimensions::Dict
    metadata::OrderedDict
end

# Tables.jl implementation
Tables.istable(::Type{<:Datatable}) = true
Tables.columnaccess(::Type{<:Datatable}) = true
Tables.columns(dt::Datatable) = dt

# getter methods to avoid getproperty clash
name(dt::Datatable) = getfield(dt, :name)
data(dt::Datatable) = getfield(dt, :data)
source(dt::Datatable) = getfield(dt, :source)
dimensions(dt::Datatable) = getfield(dt, :dimensions)
metadata(dt::Datatable) = getfield(dt, :metadata)

# Methods for Tables.jl acces to data
Tables.columnnames(dt::Datatable) = propertynames(data(dt))
Tables.getcolumn(dt::Datatable, i::Int)	 = getfield(data(dt), i)
Tables.getcolumn(dt::Datatable, nm::Symbol) = getproperty(data(dt), nm)

"""parsedimensions(dt) gathers basic information for building dimension columns and their category values.
Information is organized in a Vector of NamedTuples where each tuple has column label, categories' names and size""" 
function parsedimensions(jsondata::JSONStatData)
    
    dt= jsondata.js
    dims = Vector()

    for (id, sz) in zip(dt.id, dt.size) # Constructs a Vector of Tuples containing (dimension, labels, size)

        # 1. Column names are the label of each dimension if it has one, if not the id is used
        nm = haskey(dt.dimension[id],:label) ? Symbol(dt.dimension[id][:label]) : dt.dimension[id]  

        # 2. Gather categories in the correct order
        categories = dt.dimension[id].category

        if !haskey(categories, :index) # if there is no index, categories are the labels
            push!(dims,(; id = id,label = nm, categories = collect(values(dt.dimension[id]["category"]["label"])),size=sz))
        else # if there is an index
            if isa(categories.index,JSON3.Object) # if categories is an Object dictionary we sort it. If it is an array it is alreary sorted
                order = sortperm(collect(values(categories.index)))
                orderedkeys = collect(keys(categories.index))[order] # sorts categories by index
            else
                orderedkeys = categories.index
            end

            if haskey(categories, :label) # if there is also labels
                push!(dims,
                    (; id = id, label = nm, categories = [categories.label[idx] for idx in orderedkeys], size = sz))
            else
                push!(dims, (; id = id, label = nm, categories = orderedkeys, size=sz))
            end
        end
    end #for

    return dims
end

"""Stores dataset metadata"""
function metadata(jsondata::JSONStatData) 
    dt = jsondata.js
    mt = OrderedDict()
    for fᵢ in metadatafields
        haskey(dt,fᵢ) && (mt[fᵢ] = dt[fᵢ])
    end
    mt
end

"""Stores dimension and category metadata"""
function dimensiondata(jsondata::JSONStatData)
    dt = jsondata.js
    dd = OrderedDict()  # TODO
end

"""Array from JSONStat sparse values delivered as a dictionary"""
function dicttovect(dt::JSONStatData, l::Int)
    v = Vector{Any}(missing, l)
    for (k, j) in dt.value
        v[parse(Int, k) + 1] = j #JSONStat uses zero-indexing
    end
    return v
end

"""
    JSONStat.read(js::Union{Vector{UInt8},String})

Constructs JSONStat.datatable compatible with Tables.jl

# Example
```julia  
    HTTP.get("https://json-stat.org/samples/canada.json").body |> JSONStat.read
```

"""
function read(json::Union{Vector{UInt8},String})

    jsondata = JSONStatData(JSON3.read(js))

    if dt.js.class == "dataset"
        readdataset(jsondata)
    elseif dt.js.class == "collection"
        error("Collection class not supported yet")
    else
        error("Class not supported yet")
    end
end

function readdataset(dt::JSONStatData)     

    dt = jsondata.js
    l = prod(dt.size)
    dim = parsedimensions(dt)
    (typeof(dt.value) == Dict{String,Any}) ? data = dicttovect(dt, l) : data = dt.value
    mask = @. !ismissing(data) # 1 for non missing values

    headers = Vector{Symbol}(undef,length(dt.size)+1)
    values = Vector{Any}(undef,length(dt.size)+1)

    # Dimension columns
    f = 1
    for (i,dᵢ) in enumerate(dim)
        headers[i] = dᵢ.label
        values[i] = repeat(dᵢ.categories; inner=div(l, dᵢ.size * f), outer=f)[mask]
        f = f * dᵢ.size
    end

    # Value column
    headers[end] = :Value
    values[end] = data[mask]

    return Datatable(dt.label,
                    (;zip(headers,values)...), # NamedTuple of values for easy Tables.jl compliance
                    dt.source,
                    Dict(String(j.label) => j for j in dim),
                    metadata(dt)) 
end

function Base.show(io::IO,dt::Datatable) 
    println("\n ",length(data(dt)),"x",length(data(dt)[1])," JSONStat.Datatable")
    printstyled(" ",name(dt),"\n"; bold=true)
    println(" ",source(dt))
    pretty_table(dt; alignment = :l)
end

end # Module
