module JSONStat

# TODO: Units
# TODO: Deal with 'Collection' class
# TODO: Child

using JSON3, StructTypes, Tables, PrettyTables

export datatable

# Struct for wrapping the JSONStat response
mutable struct Dataset 
    version::String
    class::String
    label::String
    note::Any
    id::Array{String}
    size::Array{Int}
    dimension::Any
    value::Any
    status::Any
    extension::Any
    href::Any
    link::Any
    source::String
    role::Any

    Dataset() = new()
end

StructTypes.StructType(::Type{Dataset}) = StructTypes.Mutable() # For JSON3. Mutable as JSONStat has 

# Wrapper for defining specific behaviours (show...)
struct datatable <: Tables.AbstractColumns
    name::String
    data::NamedTuple
    dimensions::Vector
end

Tables.istable(::Type{<:datatable}) = true
Tables.columnaccess(::Type{<:datatable}) = true
Tables.columns(dt::datatable) = dt

# getter methods to avoid getproperty clash
name(dt::datatable) = getfield(dt, :name)
data(dt::datatable) = getfield(dt, :data)
dimensions(dt::datatable) = getfield(dt, :dimensions)

# Methods for Tables.jl acces to data
Tables.columnnames(dt::datatable) = propertynames(data(dt))
Tables.getcolumn(dt::datatable, i::Int)	 = getfield(data(dt), i)
Tables.getcolumn(dt::datatable, nm::Symbol) = getproperty(data(dt), nm)


"""Basic information on dataset dimensions:label, categories, size"""
function parsedimensions(dt::Dataset)
    
    dims = Vector()

    for (id, sz) in zip(dt.id, dt.size) # Constructs a Vector of Tuples containing (dimension, labels, size)
        nm = Symbol(dt.dimension[id]["label"])
        categories = dt.dimension[id]["category"]

        #TODO: Rediseñar esto según JSONStat.org. Puede ser sólo 'label', solo 'index' o los dos

        if haskey(categories, "label") && haskey(categories, "index") # Categories have 'label' and 'index'
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
        elseif haskey(categories, "label") # Categories only have 'label'
                push!(dims,(; nm => collect(values(dt.dimension[id]["category"]["label"])),size=sz))
        else # Categories only have 'index'
            if typeof(categories["index"]) == Dict{String,Any} # index is a  Dict
                order = sortperm(collect(values(categories["index"])))
                orderedkeys = collect(keys(categories["index"]))[order] # orders categories by index
                push!(dims, (; nm => orderedkeys, size=sz))
            else # index is a Vector
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

    dt.class != "dataset" && error("Currently only 'dataset' class is supported")
     
    l = prod(dt.size)
    dim = parsedimensions(dt)
    (typeof(dt.value) == Dict{String,Any}) ? data = dicttovect(dt, l) : data = dt.value
    mask = @. !ismissing(data) # 1 for non missing values

    temp = NamedTuple() # Empty NamedTuple

    # Dimension columns
    f = 1
    for dᵢ in dim
        values = repeat(dᵢ[1]; inner=div(l, dᵢ.size * f), outer=f)[mask]
        temp = merge((; keys(dᵢ)[1] => values), temp)

        f = f * dᵢ.size
    end

    return datatable(dt.label,
                    merge(temp, (; Value=data[mask])),
                    dim) 
end

function Base.show(io::IO,dt::datatable) 
    println("\n ",length(data(dt)),"x",length(data(dt)[1])," JSONStat.datatable")
    printstyled(" ",name(dt),"\n"; bold=true)
    pretty_table(dt;alignment=:l)
end


end # Module
