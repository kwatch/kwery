###
### $Rev$
### $Release: $
### $Copyright$
### $License$
###


module Kwery


  module Model

    def [](name)
      return instance_variable_get("@#{name}")
    end

    def []=(name, value)
      (@__old__ ||= {})[name.to_sym] = self[name]
      instance_variable_set("@#{name}", value)
    end

    def self.included(klass)
      klass.class_eval do
        arr = []
        klass.name.split('::')[-1].scan(/(?:[A-Z0-9]+[^A-Z]+)/) {|s| arr << s.downcase }
        @__table__ = arr.join('_')
        @__column_names__ = []
        @__columns__ = []
        class <<self
          attr_accessor :__table__
          attr_reader :__column_names__
          attr_reader :__columns__, :create_table_sql
        end
        def self.set_table_name(table_name)
          @__table__ = table_name
        end
        def __before_insert__(values)
          ## pass
        end
        def __before_update__(values)
          ## pass
        end
        def self.add_columns(*column_names)
          @__column_names__ ||= []
          @__column_names__.concat(column_names.collect {|col| col.to_sym })
          attr_accessor *column_names
          defs = column_names.collect do |col|
            <<-END
              def #{col}=(val)
                @__old__[:#{col}] = val if @__old__ && @#{col} != val
                @#{col} = val
              end
            END
          end
          defs << <<-END
              has_created_at = @__column_names__.include?(:created_at)
              has_updated_at = @__column_names__.include?(:updated_at)
              if has_created_at && has_updated_at
                def __before_insert__(values)
                  values[:created_at] = @created_at = __current_timestamp__()
                  values[:updated_at] = @updated_at = __current_timestamp__()
                end
              elsif has_created_at
                def __before_insert__(values)
                  values[:created_at] = @created_at = __current_timestamp__()
                end
              end
              if has_updated_at
                def __before_update__(values)
                  values[:updated_at] = @updated_at = __current_timestamp__()
                end
              end
          END
          self.class_eval(defs.join)
        end
        def self.create_table(table_name, options={}, &block)
          require 'kwery/table'
          self.set_table_name(table_name)
          builder = TableBuilder.new(Query.new(nil))
          builder.create_table(table_name, options, &block)
          @__options__ = options
          @__columns__ = builder.columns
          self.add_columns(*@__columns__.collect {|c| c._name })
          @__builder__ = builder
        end
        def self.to_sql(query=nil)
          return nil unless @__builder__
          return @__builder__.to_sql()
        end
      end
      return self
    end

    def initialize(*args)
      return if args.empty?
      if args.length == 1 && args.first.is_a?(Hash)
        args.first.each do |name, val|
          instance_variable_set("@#{name}", val)
        end
      else
        args.zip(self.class.__column_names__) do |val, col|
          self.instance_variable_set("@#{col}", val)
        end
      end
    end

    def to_hash
      hash = {}
      self.class.__column_names__.each do |name|
        hash[name] = instance_variable_get("@#{name}")
      end
      return hash
    end

    def to_array
      return self.class.__column_names__.collect {|name| instance_variable_get("@#{name}") }
    end

    def __selected__(query)
      @__old__ = {}
    end

    def __inserted__(query)
      @id = query.last_insert_id
      #arr = query.select_only(self.class.__table__, 'created_at') {|c| c.where(:id, @id) }
      #@created_at = @updated_at = arr.first
      @__old__ = {}
    end

    def __updated__(query)
      #arr = query.select_only(self.class.__table__, 'updated_at') {|c| c.where(:id, @id) }
      #@updated_at = arr.first
      @__old__.clear()
    end

    def __deleted__(query)
      @__old__ = nil
    end

    def __insert__(query)
      raise "Already inserted." if @__old__
      values = self.to_hash
      __before_insert__(values)
      query.insert(self.class.__table__, self.to_hash)
      __inserted__(query)
    end

    def __update__(query)
      raise "Not inserted object." if @__old__.nil?
      if !@__old__.empty?
        values = {}
        @__old__.each do |name, old_val|
          val = self.instance_variable_get("@#{name}")
          values[name] = val
        end
        __before_update__(values)
        query.update(self.class.__table__, values, self.id)
        __updated__(query)
      end
    end

    def __delete__(query)
      raise "Not inserted object." if @__old__.nil?
      query.delete(self.class.__table__, self.id)
      __deleted__(query)
    end

    def __current_timestamp__
      return :current_timestamp
      #return 'now()'
      #return Time.now
    end

  end


end


def require_model(model_name, file_name=nil)
  file_name ||= model_name.to_s
  autoload(model_name, file_name)
end
