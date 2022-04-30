module JSONStat

# TODO: add Units and Child to dimensions
# TODO: Deal with 'Collection' class
# TODO: status field

using JSON3, Tables, PrettyTables, OrderedCollections

# Fields to include in dataset metadata and dimension+category data and REPL print
const metadatafields = (:source, :updated, :note, :extension, :href, :type, :link, :error) # excluding :version, :class , :role (added to category)
const dimfields = (:note, :href, :label)
const categoryfields = (:child, :coordinates, :unit) # TODO: Add :role
const printfields = (:source, :updated) # TODO: fields to print when show()

# Output struct for defining specific show method and accessors
struct Datatable <: Tables.AbstractColumns
    name::String
    data::NamedTuple
    source::String
    dimensions::Dict
    metadata::OrderedDict
end

function Base.show(io::IO, dt::Datatable)
    println("\n ", length(data(dt)), "x", length(data(dt)[1]), " JSONStat.Datatable")
    printstyled(" ", name(dt), "\n"; bold=true)
    println(" ", source(dt))
    pretty_table(dt; alignment=:l)
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
Tables.getcolumn(dt::Datatable, i::Int) = getfield(data(dt), i)
Tables.getcolumn(dt::Datatable, nm::Symbol) = getproperty(data(dt), nm)

"""
    JSONStat.read(js::Union{Vector{UInt8},String})

Constructs JSONStat.datatable compatible with Tables.jl

# Example
```julia  
    HTTP.get("https://json-stat.org/samples/canada.json").body |> JSONStat.read
```
"""
function read(json::Union{Vector{UInt8},String})

    jstat = JSON3.read(json)
    # TODO: validate(jstat)

    if jstat.class == "dataset"
        readdataset(jstat)
    elseif jstat.class == "collection"
        error("Collection class not supported yet")
    else
        error("Class not supported yet")
    end
end

function readdataset(jstat::JSON3.Object)

    l = prod(jstat.size)
    dim = parsedimensions(jstat)
    (typeof(jstat.value) == Dict{String,Any}) ? data = dicttovect(jstat, l) : data = jstat.value
    mask = @. !ismissing(data) # 1 for non missing values

    headers = Vector{Symbol}(undef, length(jstat.size) + 1)
    values = Vector{Any}(undef, length(jstat.size) + 1)

    # Dimension columns
    f = 1
    for (i, dᵢ) in enumerate(dim)
        headers[i] = dᵢ.label
        values[i] = repeat(dᵢ.categories; inner=div(l, dᵢ.size * f), outer=f)[mask]
        f = f * dᵢ.size
    end

    # Value column
    headers[end] = :Value
    values[end] = data[mask]

    return Datatable(jstat.label,
        (; zip(headers, values)...), # NamedTuple of values for easy Tables.jl compliance
        jstat.source, # WARNING: :source is optional
        Dict(String(j.label) => j for j in dim),
        metadata(jstat))
end


"""parsedimensions(jstat) gathers basic information for building dimension columns and their category values.
Information is organized in a Vector of NamedTuples where each tuple has column label, a vector of categories labels/keys and size"""
function parsedimensions(jstat::JSON3.Object)

    dims = Vector()

    for (id, sz) in zip(jstat.id, jstat.size) # Constructs a Vector of Tuples containing (dimension, labels, size)

        # 1. Column names are the label of each dimension if it has one, if not the id is used
        nm = haskey(jstat.dimension[id], :label) ? Symbol(jstat.dimension[id][:label]) : jstat.dimension[id]

        # 2. Gather categories in the correct order
        categories = jstat.dimension[id].category

        if !haskey(categories, :index) # if there is no index, categories are the labels
            push!(dims, (; id=id, label=nm, categories=collect(values(jstat.dimension[id]["category"]["label"])), size=sz))
        else # if there is an index
            if isa(categories.index, JSON3.Object) # if categories is an Object dictionary we sort it. If it is an array it is alreary sorted
                order = sortperm(collect(values(categories.index)))
                orderedkeys = collect(keys(categories.index))[order] # sorts categories by index
            else
                orderedkeys = categories.index
            end

            if haskey(categories, :label) # if there is also labels
                push!(dims,
                    (; id=id, label=nm, categories=[categories.label[idx] for idx in orderedkeys], size=sz))
            else
                push!(dims, (; id=id, label=nm, categories=orderedkeys, size=sz))
            end
        end
    end #for

    return dims
end

"""Stores dataset metadata"""
function metadata(jstat::JSON3.Object)

    mt = OrderedDict()
    for fᵢ in metadatafields
        haskey(jstat, fᵢ) && (mt[fᵢ] = jstat[fᵢ])
    end
    mt
end

"""Stores dimension and category metadata"""
function dimensiondata(jstat::JSON3.Object)
      dd = OrderedDict()  # TODO
end

"""Helper function. Creates an Array from JSONStat when sparse values are delivered as a dictionary"""
function dicttovect(jstat::JSON3.Object, l::Int)
    v = Vector{Any}(missing, l)
    for (k, j) in jstat.value
        v[parse(Int, k)+1] = j #JSONStat uses zero-indexing
    end
    return v
end

end # Module
