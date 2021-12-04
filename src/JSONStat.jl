module JSONStat

# TODO: add Units and Child to dimensions
# TODO: Deal with 'Collection' class

using JSON3, StructTypes, Tables, PrettyTables
export dimensions, metadata

# Struct for wrapping the JSONStat response. Really necessary?
mutable struct Dataset 
    version::String
    class::String
    source::String
    label::String
    id::Array{String}
    size::Array{Int}
    dimension::Any
    value::Any
    status::Any
    note::Any
    extension::Any
    href::Any
    link::Any
    role::Any

    Dataset() = new()
end

StructTypes.StructType(::Type{Dataset}) = StructTypes.Mutable() # For JSON3. Mutable as JSONStat has 

# Wrapper for defining specific show method and accessors
struct Datatable <: Tables.AbstractColumns
    name::String
    data::NamedTuple
    source::String
    dimensions::Dict
    metadata::Dict
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


"""Basic information on dataset dimensions:label, categories, size""" # TODO: Unit, Child
function parsedimensions(dt::Dataset)
    
    dims = Vector()

    for (id, sz) in zip(dt.id, dt.size) # Constructs a Vector of Tuples containing (dimension, labels, size)
        nm = Symbol(dt.dimension[id]["label"])
        categories = dt.dimension[id]["category"]

        if haskey(categories, "label") && haskey(categories, "index") # Categories have 'label' and 'index'
            if typeof(categories["index"]) == Dict{String,Any}
                order = sortperm(collect(values(categories["index"])))
                orderedkeys = collect(keys(categories["index"]))[order] # orders categories by index
                push!(dims,
                    (; id = id, label = nm, categories = [categories["label"][idx] for idx in orderedkeys], size = sz)) # Labels ordered by index
            else
                push!(dims,
                    (; id = id,label = nm, categories = [categories["label"][idx] for idx in categories["index"]],
                    size=sz)) # Iterado según Index
            end
        elseif haskey(categories, "label") # Categories only have 'label'
                push!(dims,(; id = id,label = nm, categories = collect(values(dt.dimension[id]["category"]["label"])),size=sz))
        else # Categories only have 'index'
            if typeof(categories["index"]) == Dict{String,Any} # index is a  Dict
                order = sortperm(collect(values(categories["index"])))
                orderedkeys = collect(keys(categories["index"]))[order] # orders categories by index
                push!(dims, (; id = id, label = nm, categories = orderedkeys, size=sz))
            else # index is a Vector
                push!(dims, (; id = id, label = nm, categories = dt.dimension[id]["category"]["index"], size=sz))
            end
        end
    end

    return dims
end

"""Stores metadata"""
function metadata(dt::Dataset) 

    fields = (:version, :class, :note, :extension, :href, :link, :role)
    mt = Dict()
    for fᵢ in fields
        isdefined(dt,fᵢ) && (mt[fᵢ] = getfield(dt,fᵢ))
    end
    mt
end

"""Array from a sparse JSONStat values"""
function dicttovect(dt::Dataset, l::Int)
    v = Vector{Any}(missing, l)
    for (k, j) in dt.value
        v[parse(Int, k) + 1] = j #JSONStat uses zero-indexing
    end
    return v
end

"""
    SMDX.read(js::Union{Vector{UInt8},String})

Constructs JSONStat.datatable compatible with Tables.jl

# Example
```julia  
    HTTP.get("https://json-stat.org/samples/canada.json").body |> JSONStat.read
```

"""
function read(js::Union{Vector{UInt8},String})
          
    dt = JSON3.read(js,Dataset)

    dt.class != "dataset" && error("Only 'dataset' class supported")
     
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
