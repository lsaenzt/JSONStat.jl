module JSONStat

using StructTypes

export datatable

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

StructTypes.StructType(::Type{Dataset}) = StructTypes.Mutable()

"""Basic information on dataset dimensions"""
function parsedimensions(dt::Dataset)
    dims = Vector()

    for (id, sz) in zip(dt.id, dt.size) # Constructs a Vector of Tuples containing (dimension, label, size)
        nm = Symbol(dt.dimension[id]["label"])
        categories = dt.dimension[id]["category"]

        if haskey(categories, "label")
            if typeof(categories["index"]) == Dict{String,Any}
                order = sortperm(collect(values(categories["index"])))
                orderedkeys = collect(keys(categories["index"]))[order] # orders categories by index
                push!(dims,
                      (; nm => [categories["label"][idx] for idx in orderedkeys], size=sz)) # Label ordered by index
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
function datatable(dt::Dataset)
    l = prod(dt.size)
    dim = parsedimensions(dt)
    (typeof(dt.value) == Dict{String,Any}) ? data = dicttovect(dt, l) : data = dt.value
    mask = @. !ismissing(data) # 1 for non missing values

    temp = (;) # Empty NamedTuple

    f = 1
    for dᵢ in dim
        if dᵢ.size > 1 # Ignoring dimensions with one value
            values = repeat(dᵢ[1]; inner=div(l, dᵢ.size * f), outer=f)[mask]
            temp = merge((; keys(dᵢ)[1] => values), temp)

            f = f * dᵢ.size
        end
    end

    return merge(temp, (; Value=data[mask])) # A NamedTuple of data meets Tables.jl interface with its default implementation
end

end #Module
