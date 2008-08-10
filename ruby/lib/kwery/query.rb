###
### $Rev$
### $Release: $
### $Copyright$
### $License$
###


module Kwery


  module QueryBuilder

    attr_accessor :_table, :_where, :_order_by, :_group_by, :_having, :_limit, :_join

    def clear
      @_table = @_where = @_order_by = @_group_by = @_having = @_limit = @_join = nil
    end

    def escape_string(val)
      raise NotImplementedError.new("#{self.class.name}#escape_string(): not implemented yet.")
    end

    def quote_keyword(word)
      raise NotImlementedError.new("#{self.class.name}#quote_keyword(): not implemented yet.")
    end

    def quote_value(val)
      case val
      when nil     ;  return 'null'
      when String  ;  return "'#{escape_string(val)}'"
      when Numeric ;  return val.to_s
      when Time    ;  return val.strftime("'%Y-%m-%d %H:%M:%S'")
      when Symbol  ;  return val.to_s
      else         ;  return val.to_s
      end
    end

    def build_select_sql(arg, id=nil)
      if id
        sql = "select #{arg || '*'} from #{@_table} where id = #{quote_value(id)}"
      else
        sql = "select #{arg || '*'} from #{@_table}"
        sql << @_join if @_join
        sql << ' where '    << @_where    if @_where
        sql << ' group by ' << @_group_by if @_group_by
        sql << ' having '   << @_having   if @_having
        sql << ' order by ' << @_order_by if @_order_by
        sql << " limit #{@_limit[0]}, #{@_limit[1]}" if @_limit
      end
      return sql
    end

    def build_insert_sql(values)
      if values.is_a?(Array)
        s = values.collect {|v| quote_value(v) }.join(', ')
        sql = "insert into #{@_table} values (#{s})"
      elsif values.is_a?(Hash)
        keys = []
        vals = []
        values.each do |k, v|
          keys << quote_keyword(k)
          vals << quote_value(v)
        end
        sql = "insert into #{@_table}(#{keys.join(', ')}) values (#{vals.join(', ')})"
      #else
      #  keys = []
      #  vals = []
      #  values.instance_variables.each do |name|
      #    k = name[1..-1]
      #    v = values.instance_variable_get(name)
      #    keys << quote_keyword(k)
      #    vals << quote_value(v)
      #  end
      #  sql = "insert into #{@_table}(#{keys.join(', ')}) values (#{vals.join(', ')})"
      else
        raise ArgumentError.new('invalid arguments.')
      end
      return sql
    end

    def build_update_sql(values, id=nil)
      if values.is_a?(Array)
        s = values.collect {|k, v| "#{quote_keyword(k)}=#{quote_value(v)}" }.join(', ')
        sql = "update #{@_table} set #{s}"
      elsif values.is_a?(Hash)
        s = values.collect {|k, v| "#{quote_keyword(k)}=#{quote_value(v)}" }.join(', ')
        sql = "update #{@_table} set #{s}"
      end
      if !id.nil?
        sql << " where id = #{quote_value(id)}"
      elsif @_where
        sql << ' where ' << @_where
      end
      return sql
    end

    def build_delete_sql(id=nil)
      sql = "delete from #{@_table}"
      if !id.nil?
        sql << " where id = #{quote_value(id)}"
      elsif @_where
        sql << ' where ' << @_where
      end
      return sql
    end

    def where(key, val=UNDEFINED)
      if @_where
        @_where << ' and ' << _factor(key, val)
      else
        @_where = _factor(key, val)
      end
      return self
    end

    def and_where(key, val=UNDEFINED)
      @_where << ' and ' << _factor(key, val)
      return self
    end

    def or_where(key, val=UNDEFINED)
      @_where << ' or ' << _factor(key, val)
      return self
    end

    def or_where!(key, val=UNDEFINED)
      @_where = "(#{@_where}) or #{_factor(key, val)}"
      return self
    end

    def and_where!(key, val=UNDEFINED)
      @_where = "(#{@_where}) and #{_factor(key, val)}"
      return self
    end

    def having(key, val=UNDEFINED)
      if @_having
        @_having << ' and ' << _factor(key, val)
      else
        @_having = _factor(key, val)
      end
      return self
    end

    def and_having(key, val=UNDEFINED)
      @_having << ' and ' << _factor(key, val)
      return self
    end

    def or_having(key, val=UNDEFINED)
      @_having << ' or ' << _factor(key, val)
      return self
    end

    def and_having!(key, val=UNDEFINED)
      @_having = "(#{@_having}) and #{_factor(key, val)}"
      return self
    end

    def or_having!(key, val=UNDEFINED)
      @_having = "(#{@_having}) or #{_factor(key, val)}"
      return self
    end

    def _factor(key, val)  # :nodoc:
      if key.is_a?(String)
        if val.equal?(UNDEFINED) ;  return key
        elsif val.nil?           ;  return "#{quote_keyword(key)} is null"
        elsif val.is_a?(Array)   ;  return (key % val.collect {|v| quote_value(v) })
        elsif val.is_a?(Hash)    ;  h = {}; val.each {|k, v| h[k] = quote_value(v) }
                                    return (key % h)
        else
          a = key.split(' ', 2)
          return a.length == 1 ? "#{quote_keyword(key)} = #{quote_value(val)}" \
                               : "#{quote_keyword(a.first)} #{a.last} #{quote_value(val)}"
        end
      elsif key.is_a?(Symbol)
        if val.equal?(UNDEFINED) ;  raise ArgumentError.new("value is required.")
        elsif val.nil?           ;  return "#{quote_keyword(key)} is null"
        elsif val.is_a?(Array)   ;  raise ArgumentError.new("array is not acceptable.")
        elsif val.is_a?(Hash)    ;  raise ArgumentError.new("hash is not acceptable.")
        else                     ;  return "#{quote_keyword(key)} = #{quote_value(val)}"
        end
      elsif key.is_a?(Hash)
        unless val.equal?(UNDEFINED)
          raise ArgumentError.new("2nd argument shouldn't be specified.")
        end
        return key.collect {|key, val| "#{key} = #{quote_value(val)}" }.join(' and ')
      else
        raise ArgumentError.new('invalid where-clause.')
      end
    end
    protected :_factor

    def order_by(*args)
      @_order_by = args.join(', ')
      return self
    end

    def order_by_desc(*args)
      @_order_by = args.join(', ')
      @_order_by << ' desc'
      return self
    end

    def group_by(*args)
      @_group_by = args.join(', ')
      return self
    end

    def limit(offset, count)
      @_limit = [offset, count]
      return self
    end

    def join(join_table, column, primary_key='id')
      _build_join('join', join_table, column, primary_key || 'id')
      return self
    end

    def left_outer_join(join_table, column, primary_key='id')
      _build_join('left outer join', join_table, column, primary_key || 'id')
      return self
    end

    def left_inner_join(join_table, column, primary_key='id')
      _build_join('left inner join', join_table, column, primary_key || 'id')
      return self
    end

    def right_outer_join(join_table, column, primary_key='id')
      _build_join('right outer join', join_table, column, primary_key || 'id')
      return self
    end

    def right_inner_join(join_table, column, primary_key='id')
      _build_join('right inner join', join_table, column, primary_key || 'id')
      return self
    end

    def _build_join(phrase, join_table, column, primary_key)
      @_join ||= ''
      @_join << " #{phrase} #{join_table} on #{@_table}.#{column} = #{join_table}.#{primary_key}"
      #arr = join_table.split(' ')
      #@_join << " #{phrase} #{arr.first} on #{@_table}.#{column} = #{arr.last}.#{primary_key}"
    end
    private :_build_join

  end


  module QueryExecutor

    attr_accessor :conn, :builder, :debug, :table_prefix

    def set_table_name(table)
      @builder._table = @table_prefix ? @table_prefix + table : table
    end
    protected :set_table_name

    def set_table(table)
      if    table.is_a?(String) ; set_table_name(table)
      elsif table.is_a?(Class)  ; set_table_name(table.__table__)
      elsif table.is_a?(Model)  ; raise ArgumentError.new("Model not allowed.")
      else                      ; raise ArgumentError.new("invalid table object.")
      end
    end
    private :set_table

    def get(table, id=nil)
      set_table(table)
      yield(@builder) if block_given?
      sql = @builder.build_select_sql('*', id)
      result = execute(sql)
      @builder.clear()
      if table.is_a?(String)
        ret = result.fetch_hash()
      else
        #assert table.is_a?(Class)
        #ret = result.fetch_object(table)
        ret = table.new(result.fetch_hash())
        ret.__selected__(self)
      end
      result.free() if @auto_free
      return ret
    end

    def _collect_models(result, model_class, list=[])  # :nodoc:
      result.each_hash do |hash|
        list << (model = model_class.new(hash))
        model.__selected__(self)
      end
      return list
    end
    protected :_collect_models

    def get_all(table, key=UNDEFINED, val=UNDEFINED)
      set_table(table)
      @builder.where(key, val) unless key.equal?(UNDEFINED)
      yield(@builder) if block_given?
      sql = @builder.build_select_sql('*')
      result = execute(sql)
      @builder.clear()
      list = []
      if table.is_a?(String) ; result.each_hash {|hash| list << hash }
      else                   ; _collect_models(result, table, list)
      end
      result.free() if @auto_free
      return list
    end

    def select(table, columns=nil, klass=nil)
      set_table(table)
      yield(@builder) if block_given?
      sql = @builder.build_select_sql(columns)
      result = execute(sql)
      @builder.clear()
      list = []
      if klass.nil?
        if    table.is_a?(String)   ; result.each_hash {|hash| list << hash }
        else                        ; _collect_models(result, table, list)
        end
      else
        if    klass == Hash         ; result.each_hash {|hash| list << hash }
        elsif klass == Array        ; result.each_array {|arr| list << arr }
        elsif klass.include?(Model) ; _collect_models(result, table, list)
        else
          #result.each_object(klass) {|obj| list << obj }
          result.each_hash do |hash|
            list << (obj = klass.new)
            hash.each {|k, v| obj.instance_variable_set("@#{k}", v) }
            #hash.each {|k, v| obj.__send__("#{k}=", v) }
          end
        end
      end
      result.free() if @auto_free
      return list
    end

    def select_only(table, column)
      set_table(table)
      yield(@builder) if block_given?
      sql = @builder.build_select_sql(column)
      result = execute(sql)
      @builder.clear()
      #return result.collect {|arr| arr.first }
      list = []
      result.each_array {|arr| list << arr.first }
      result.free() if @auto_free
      return list
    end

    def insert(table, values)
      return table.__insert__(self) if table.is_a?(Model)
      set_table(table)
      sql = @builder.build_insert_sql(values)
      @builder.clear()
      return execute(sql)
    end

    def last_insert_id
      return @conn.insert_id
    end

    def update(table, values, id=nil)
      return table.__update__(self) if table.is_a?(Model)
      set_table(table)
      yield(@builder) if block_given?
      unless id || @_where
        raise ArgumentError.new("update condition is reqiured.")
      end
      sql = @builder.build_update_sql(values, id)
      @builder.clear()
      return execute(sql)
    end

    def update_all(table, values)
      set_table(table)
      sql = @builder.build_update_sql(values, nil)
      @builder.clear()
      return execute(sql)
    end

    def delete(table, id=nil)
      return table.__delete__(self) if table.is_a?(Model)
      set_table(table)
      yield(@builder) if block_given?
      unless id || @_where
        raise ArgumentError.new("delete condition is reqiured.")
      end
      sql = @builder.build_delete_sql(id)
      @builder.clear()
      return execute(sql)
    end

    def delete_all(table)
      set_table(table)
      sql = @builder.build_delete_sql(nil)
      @builder.clear()
      return execute(sql)
    end

    def execute(sql)
      $stderr.puts sql if @debug
      return @conn.query(sql)
    end

    def insert_object(obj)
      obj.__insert__(self)
    end

    def update_object(obj)
      obj.__update__(self)
    end

    def delete_object(obj)
      obj.__delete__(self)
    end

    def transaction
      return start_transaction() unless block_given?
      start_transaction()
      yield(@builder)
      return commit()
    rescue Exception => ex
      rollback()
      raise ex
    end

    def start_transaction
      return execute('start transaction')
    end

    def commit
      return execute('commit')
    end

    def rollback
      return execute('rollback')
    end

  end


  module QueryHelper

    def solve_belongs_to(items, from_attr, from_key, to_table, to_key='id', not_null=false)
      #solve_has_one(items, from_attr, to_key, to_table, from_key, not_null)
      list = items.collect {|item| item[from_key]}
      list = list.select {|v| v } unless not_null
      yield(self) if block_given?
      cond = "#{to_key} in (#{list.join(',')})"
      ref_items = self.get_all(to_table) {|c| c.where(cond) }
      hash = ref_items.index_by(to_key)
      items.each do |item|
        item[from_attr] = hash[item[from_key]]
        #v = item[from_key]
        #item[from_attr] = hash[v]
        #hash[v][to_attr] = item if hash[v]
      end
      nil
    end

    def solve_has_one(items, attr, from_key, from_table, to_key='id', not_null=false)
      solve_belongs_to(items, attr, to_key, from_table, from_key, not_null)
      #list = items.collect {|item| item[to_key] }
      #list = list.select {|v| v } unless not_null
      #yield(self) if block_given?
      #cond = "#{from_key} in (#{list.join(',')})"
      #ref_items = self.get_all(from_table) {|c| c.where(cond) }
      #hash = ref_items.index_by(from_key)
      #items.each do |item|
      #  item[attr] = hash[item[to_key]]
      #  #v = item[to_key]
      #  #item[to_attr] = hash[v]
      #  #hash[v][from_attr] = item if hash[v]
      #end
    end

    def solve_has_many(items, attr, from_key, from_table, to_key='id', not_null=false)
      list = items.collect {|item| item[to_key] }
      list = list.select {|v| v } unless not_null
      yield(self) if block_given?
      cond = "#{from_key} in (#{list.join(',')})"
      ref_items = self.get_all(from_table) {|c| c.where(cond) }
      hash = ref_items.group_by(from_key)
      if not_null
        items.each do |item|
          item[attr] = hash[item[to_key]] || []
        end
      else
        items.each do |item|
          item[attr] = (v = item[to_key]).nil? ? nil : (hash[v] || [])
        end
      end
      nil
    end

  end


  class Query
    include QueryBuilder
    include QueryExecutor
    include QueryHelper

    class <<self
      attr_accessor :default_class
    end

    def initialize(conn)
      @conn = conn
      @builder = self
      @auto_free = true
    end

    class <<self
      alias __new__ new
    end

  end


end
