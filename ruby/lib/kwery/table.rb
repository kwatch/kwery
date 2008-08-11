###
### $Rev$
### $Release: $
### $Copyright$
### $License$
###

module Kwery


  class Column

    class NULL; end

    class <<self
      alias __new__ new
    end

#    def new(*args)
#      return self.__new__(*args)
#    end

    def initialize(name, type, width, width2)
      @_name = name
      @_type = type
      @_width = width
      @_width2 = width2
    end

    attr_reader :_name, :_type, :_width, :_width2
    attr_reader :_not_null, :_primary_key, :_serial, :_unique, :_default, :_references, :_on_update

    def not_null
      @_not_null = true
      return self
    end

    def primary_key
      @_primary_key = true
      return self
    end

    def auto_increment
      @_serial = true
      return self
    end

    def serial
      @_serial = true
      return self
    end

    def unique
      @_unique = true
      return self
    end

    def default(val)
      @_default = val.nil? ? Column::NULL : val
      return self
    end

    def references(table, column=:id)
      @_references = [table, column]
      return self
    end

    def on_update(arg)
      @_on_update = arg
      return self
    end

    def to_sql(query)
      s = _build_column_decl(query)
      s << " primary key" if @_primary_key
      s << " not null" if @_not_null
      s << _build_serial_constraint(query) if @_serial
      s << " unique" if @_unique
      s << " default null" if @_default.equal?(Column::NULL)
      s << " default #{query.quote_value(@_default)}" if !@_default.nil?
      s << " on update #{query.quote_value(@_on_update)}" if !@_on_update.nil?
      r = @_references
      s << " references #{query.quote_keyword(query.to_table_name(r[0]))}(#{r[1]})" if r
      return s
    end

    def _build_column_decl(query)
      s = @_width2 ? "(#{@_width}, #{@_width2})" : @_width ? "(#{@_width})" : ""
      return "%-18s %-15s" % [query.quote_keyword(@_name), "#{@_type}#{s}"]
    end

    def _build_serial_constraint(query)
      return @_serial ? " auto_increment" : nil
    end

  end


  class TableBuilder

    class <<self
      alias __new__ new
    end

    def initialize(query=nil)
      @query = query || Query.new(nil)
    end
    attr_accessor :query, :table_name, :options, :columns

    def create_table(table_name, options={}, &block)
      @table_name = table_name
      @options = options
      define_columns(&block) if block
      return to_sql()
    end

    def define_columns(&block)
      @columns = []
      before()
      block.call(self)
      after()
      return @columns
    end

    def to_sql
      q = @query
      s =  "create table #{q.quote_keyword(q.to_table_name(@table_name))} (\n  "
      s << @columns.collect {|column| column.to_sql(q) }.join(",\n  ") << "\n)"
      return s
    end

    def before
      #self.integer(:id) {|c| c.primary_key.serial }
    end

    def after
      #self.timestamp(:created_at) {|c| c.not_null }
      #self.timestamp(:updated_at) {|c| c.not_null }
    end

    ###

    def integer(name, width=nil, &block)
      _build_column(name, :integer, width, nil, &block)
    end

    def string(name, width=nil, &block)
      _build_column(name, :varchar, width || 255, nil, &block)
    end

    def text(name, &block)
      _build_column(name, :text, nil, nil, &block)
    end

    def float(name, width=nil, width2=nil, &block)
      _build_column(name, :float, width, width2, &block)
    end

    def decimal(name, width=nil, width2=nil, &block)
      _build_column(name, :decimal, width, width2, &block)
    end

    def boolean(name, &block)
      _build_column(name, :boolean, nil, nil, &block)
    end

    def timestamp(name, &block)
      _build_column(name, :timestamp, nil, nil, &block)
    end

    def date(name, &block)
      _build_column(name, :date, nil, nil, &block)
    end

    def time(name, &block)
      _build_column(name, :time, nil, nil, &block)
    end

    def datetime(name, &block)
      _build_column(name, :datetime, nil, nil, &block)
    end

    def _build_column(name, type, width, width2, &block)  # :nodoc:
      column = Column.new(name, type, width, width2)
      block.call(column) if block
      @columns << column
      return column
    end

  end


end
