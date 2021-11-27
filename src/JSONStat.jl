module JSONStat

using JSON3, StructTypes, Tables, PrettyTables

export datatable

# Struct for wrapping the JSONStat response
# Not really necessary as JSON3.Object is almost the same but knowing the fields facilitates maintanance and development.
mutable struct Dataset 
    version::String
    label::String
    id::Array{String}
    size::Array{Int}
    extension::Any
    value::Any
    status::Any
    dimension::Any
    class::String
    href::String
    source::String
    role::Any

    Dataset() = new()
end

StructTypes.StructType(::Type{Dataset}) = StructTypes.Mutable() # For JSON3

# Wrapper for defining specific behaviours (show...)
struct datatable
    name::String
    columns::NamedTuple
end

Tables.istable(::Type{<:datatable}) = true
Tables.columnaccess(::Type{<:datatable}) = true
Tables.columns(dt::datatable) = dt.columns

# name(dt::datatable) = getfield(dt, :name)

"""Basic information on dataset dimensions"""
function parsedimensions(dt::Dataset)
    
    dims = Vector()

    for (id, sz) in zip(dt.id, dt.size) # Constructs a Vector of Tuples containing (dimension, labels, size)
        nm = Symbol(dt.dimension[id]["label"])
        categories = dt.dimension[id]["category"]

        if haskey(categories, "label")
            if typeof(categories["index"]) == Dict{String,Any}
                order = sortperm(collect(values(categories["index"])))
                orderedkeys = collect(keys(categories["index"]))[order] # orders categories by index
                push!(dims,
                      (; nm => [categories["label"][idx] for idx in orderedkeys], size=sz)) # Labels ordered by index
            else
                push!(dims,
                      (; nm => [categories["label"][idx] for idx in categories["index"]],
                       size=sz)) # Iterado según Index
            end
        else
            if typeof(categories["index"]) == Dict{String,Any}
                order = sortperm(collect(values(categories["index"])))
                orderedkeys = collect(keys(categories["index"]))[order] # orders categories by index
                push!(dims, (; nm => orderedkeys, size=sz))
            else
                push!(dims, (; nm => dt.dimension[id]["category"]["index"], size=sz))
            end
        end
    end

    return dims
end

"""Array from a sparse JSONStat values"""
function dicttovect(dt::Dataset, l::Int)
    v = Vector{Any}(missing, l)
    for (k, j) in dt.value
        v[parse(Int, k) + 1] = j #JSONStat uses zero-indexing
    end
    return v
end

"""Constructs a NameTuple with columnames => values for Tables.jl compliance"""
function read(js::Union{Vector{UInt8},String})
     
    dt = JSON3.read(js,Dataset)
     
    l = prod(dt.size)
    dim = parsedimensions(dt)
    (typeof(dt.value) == Dict{String,Any}) ? data = dicttovect(dt, l) : data = dt.value
    mask = @. !ismissing(data) # 1 for non missing values

    temp = (;) # Empty NamedTuple

    # Dimension columns
    f = 1
    for dᵢ in dim
        if dᵢ.size > 1 # Ignoring dimensions with one value
            values = repeat(dᵢ[1]; inner=div(l, dᵢ.size * f), outer=f)[mask]
            temp = merge((; keys(dᵢ)[1] => values), temp)

            f = f * dᵢ.size
        end
    end

    return datatable(dt.label,
                    merge(temp, (; Value=data[mask]))) # A NamedTuple of data meets Tables.jl interface with its default implementation
end

function Base.show(io::IO,dt::datatable) 
    println("\n ",length(dt.columns[1]),"x",length(dt.columns)," JSONStat.datatable") # Todo add columns and rows
    printstyled(" ",dt.name,"\n"; bold=true)
    pretty_table(dt)
end

end #Module
