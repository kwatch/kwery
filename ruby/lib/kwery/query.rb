###
### $Rev$
### $Release: $
### $Copyright$
### $License$
###


module Kwery


  module Common

    def to_table_name(table)
      t = nil
      if    table.is_a?(String) ;  t = table
      elsif table.is_a?(Class)  ;  t = table.__table__
      elsif table.is_a?(Model)  ;  raise ArgumentError.new("Model not allowed.")
      else                      ;  raise ArgumentError.new("invalid table object.")
      end
      return @table_prefix ? @table_prefix + t : t
    end

    def escape_string(val)
      raise NotImplementedError.new("#{self.class.name}#escape_string(): not implemented yet.")
    end

    def quote_keyword(word)
      raise NotImplementedError.new("#{self.class.name}#quote_keyword(): not implemented yet.")
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

  end


  class QueryBuilder
    include Common

    attr_accessor :_table, :_where, :_order_by, :_group_by, :_having, :_limit, :_join

    class <<self
      #attr_accessor :default_class
      alias __new__ new
    end

    def initialize(table_name=nil)
      @_table = table_name
    end

    def clear
      @_table = @_where = @_order_by = @_group_by = @_having = @_limit = @_join = nil
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
      if    !id.nil? ; sql << " where id = #{quote_value(id)}"
      elsif @_where  ; sql << ' where ' << @_where
      end
      return sql
    end

    def build_delete_sql(id=nil)
      sql = "delete from #{@_table}"
      if    !id.nil? ; sql << " where id = #{quote_value(id)}"
      elsif @_where  ; sql << ' where ' << @_where
      end
      return sql
    end

    def where(key, val=UNDEFINED)
      if   @_where ; @_where << ' and ' << _factor(key, val)
      else         ; @_where = _factor(key, val)
      end
      return self
    end

    alias w where

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

    def where_in(key, arr)
      #return if arr.empty?
      cond = "#{quote_keyword(key)} in (#{arr.join(',')})"
      @_where ? (@_where << ' and ' << cond) : (@_where = cond)
    end

    def where_between(key, from, to)
      cond = "#{quote_keyword(key)} between #{quote_value(from)} and #{quote_value(to)}"
      @_where ? (@_where << ' and ' << cond) : (@_where = cond)
    end

    def where_is_null(key)
      cond = "#{quote_keyword(key)} is null"
      @_where ? (@_where << ' and ' << cond) : (@_where = cond)
    end

    def where_is_not_null(key)
      cond = "#{quote_keyword(key)} is not null"
      @_where ? (@_where << ' and ' << cond) : (@_where = cond)
    end

    def having(key, val=UNDEFINED)
      if   @_having ; @_having << ' and ' << _factor(key, val)
      else          ; @_having = _factor(key, val)
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
        #elsif val.is_a?(Hash)   ;  h = {}; val.each {|k, v| h[k] = quote_value(v) }
        #                           return (key % h)
        else
          a = key.split(' ', 2)
          return a.length == 1 ? "#{quote_keyword(key)} = #{quote_value(val)}" \
                               : "#{quote_keyword(a.first)} #{a.last} #{quote_value(val)}"
        end
      elsif key.is_a?(Symbol)
        if val.equal?(UNDEFINED) ;  raise ArgumentError.new("value is required.")
        elsif val.nil?           ;  return "#{quote_keyword(key)} is null"
        elsif val.is_a?(Array)   ;  raise ArgumentError.new("array is not allowed.")
        elsif val.is_a?(Hash)    ;  raise ArgumentError.new("hash is not allowed.")
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
      table_name = to_table_name(join_table)
      @_join ||= ''
      @_join << " #{phrase} #{table_name} on #{@_table}.#{column} = #{table_name}.#{primary_key}"
      #arr = join_table.split(' ')
      #@_join << " #{phrase} #{arr.first} on #{@_table}.#{column} = #{arr.last}.#{primary_key}"
    end
    private :_build_join

  end


  class Query
    include Common

    attr_accessor :conn, :output, :table_prefix

    class <<self
      #attr_accessor :default_class
      alias __new__ new
    end

    def initialize(conn)
      @conn = conn
      @auto_free = true
    end

    def get(table, id=nil)
      builder = QueryBuilder.new(to_table_name(table))
      builder.where(:id, id) if id
      yield(builder) if block_given?
      return _get(table, builder)
    end

    ## (experimental)
    def get!(table, key, val=UNDEFINED)
      buidler = QueryBuilder.new(to_table_name(table))
      buidler.where(key, val)
      yield(builder) if block_given?
      return _get(table, builder)
    end

    def _get(table, builder)  # :nodoc:
      sql = builder.build_select_sql('*')
      result = execute(sql)
      #builder.clear()
      if table.is_a?(String)
        ret = result.fetch_hash()
      else
        #assert table.is_a?(Class)
        #ret = result.fetch_object(table)
        ret = result.fetch_hash()
        if ret
          ret = table.new(ret)
          ret.__selected__(self)
        end
      end
      result.free() if @auto_free
      return ret
    end

    def _collect_models(result, model_class, list=[])  # :nodoc:
      result.each_hash do |hash|
        if hash
          list << (model = model_class.new(hash))
          model.__selected__(self)
        else
          list << hash
        end
      end
      return list
    end
    protected :_collect_models

    def get_all(table, key=UNDEFINED, val=UNDEFINED)
      builder = QueryBuilder.new(to_table_name(table))
      builder.where(key, val) unless key.equal?(UNDEFINED)
      yield(builder) if block_given?
      return _get_all(table, builder)
    end

    ## (experimental)
    def get_all!(table, key, val=UNDEFINED)
      builder = QueryBuilder.new(to_table_name(table))
      builder.where(key, val)
      yield(builder) if block_given?
      return _get_all(table, builder)
    end

    def _get_all(table, builder)  # :nodoc:
      sql = builder.build_select_sql('*')
      result = execute(sql)
      #builder.clear()
      list = []
      if table.is_a?(String) ; result.each_hash {|hash| list << hash }
      else                   ; _collect_models(result, table, list)
      end
      result.free() if @auto_free
      return list
    end

    def select(table, columns=nil, klass=nil)
      builder = QueryBuilder.new(to_table_name(table))
      yield(builder) if block_given?
      sql = builder.build_select_sql(columns)
      result = execute(sql)
      #builder.clear()
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
      builder = QueryBuilder.new(to_table_name(table))
      yield(builder) if block_given?
      sql = builder.build_select_sql(column)
      result = execute(sql)
      #builder.clear()
      #return result.collect {|arr| arr.first }
      list = []
      result.each_array {|arr| list << arr.first }
      result.free() if @auto_free
      return list
    end

    def insert(table, values=nil)
      return table.__insert__(self) if table.is_a?(Model)
      raise ArgumentError.new("values to insert are required.") if values.nil?
      builder = QueryBuilder.new(to_table_name(table))
      sql = builder.build_insert_sql(values)
      #builder.clear()
      return execute(sql)
    end

    def last_insert_id
      return @conn.insert_id
    end

    def update(table, values=nil, id=nil)
      return table.__update__(self) if table.is_a?(Model)
      raise ArgumentError.new("values to update are required.") if values.nil?
      builder = QueryBuilder.new(to_table_name(table))
      builder.where(:id, id) if id
      yield(builder) if block_given?
      return _update(table, builder, values)
    end

    ## (experimental)
    def update!(table, values, key, val=UNDEFINED)
      builder = QueryBuilder.new(to_table_name(table))
      builder.where(key, val)
      yield(builder) if block_given?
      return _update(table, builder, values)
    end

    def _update(table, builder, values)  # :nodoc:
      unless builder._where
        raise ArgumentError.new("update condition is reqiured.")
      end
      sql = builder.build_update_sql(values)
      #builder.clear()
      return execute(sql)
    end


    def update_all(table, values)
      builder = QueryBuilder.new(to_table_name(table))
      sql = builder.build_update_sql(values)
      #builder.clear()
      return execute(sql)
    end

    def delete(table, id=nil)
      return table.__delete__(self) if table.is_a?(Model)
      builder = QueryBuilder.new(to_table_name(table))
      builder.where(:id, id) if id
      yield(builder) if block_given?
      return _delete(table, builder)
    end

    ## (experimental)
    def delete!(table, key, val=UNDEFINED)
      builder = QueryBuilder.new(to_table_name(table))
      builder.where(key, val)
      return _delete(table)
    end

    def _delete(table, builder)  # :nodoc:
      unless builder._where
        raise ArgumentError.new("delete condition is reqiured.")
      end
      sql = builder.build_delete_sql()
      #builder.clear()
      return execute(sql)
    end

    def delete_all(table)
      builder = QueryBuilder.new(to_table_name(table))
      sql = builder.build_delete_sql()
      #builder.clear()
      return execute(sql)
    end

    def execute(sql)
      #@output << sql << "\n" if @output
      #return @conn.query(sql)
      raise NotImplementedError.new("#{self.class.name}#execute(): not implemented yet.")
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
      yield(self)
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

    def _bind(items, table, item_column, table_column, attr, multiple, not_null)  # :nodoc:
      item_column = item_column.to_s
      id_list = items.collect {|item| item[item_column] }
      id_list = id_list.select {|v| v } unless not_null
      if id_list.empty?
        #self.clear
      else
        refs = self.get_all(table) {|c| c.where_in(table_column, id_list) }
        table_column = table_column.to_s
        attr = attr.to_s
        if multiple
          hash = refs.group_by(table_column)
          items.each {|item| item[attr] = hash[item[item_column]] || [] }
        else
          hash = refs.index_by(table_column)
          items.each {|item| item[attr] = hash[item[item_column]] }
        end
      end
    end

    def bind_references_to(items, table, column, attr, not_null=true)
      #yield(self) if block_given?
      #_bind(item, table, column, 'id', attr, false, not_null)
      #nil
      id_list = items.collect {|item| item[column] }
      id_list = id_list.select {|v| v } unless not_null
      return items if id_list.empty?
      refs = self.get_all(table) {|c| c.where_in(:id, id_list); yield(c) if block_given? }
      hash = refs.index_by('id')
      column = column.to_s
      items.each {|item| item[attr] = hash[item[column]] }
      return items
    end

    def bind_referenced_from(items, table, column, attr, not_null=true, multiple=true)
      #yield(self) if block_given?
      #_bind(item, table, 'id', column, attr, multiple, not_null)
      #nil
      id_list = items.collect {|item| item['id'] }
      id_list = id_list.select {|v| v } unless not_null
      return items if id_list.empty?
      refs = self.get_all(table) {|c| c.where_in(column, id_list); yield(c) if block_given? }
      if multiple
        hash = refs.group_by(column)
        items.each {|item| item[attr] = hash[item['id']] || [] }
      else
        hash = refs.index_by(column)
        items.each {|item| item[attr] = hash[item['id']] }
      end
      return items
    end

  end


  class Query
    include QueryHelper
  end


end
