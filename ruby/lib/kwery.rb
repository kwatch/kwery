###
### $Rev$
### $Release: $
### $Copyright$
### $License$
###


require 'mysql'

module Kwery


  module UNDEFINED; end

  MYSQL_KEYWORDS = %w[
          add all alter analyze and as asc asensitive
          before between bigint binary blob both by
          call cascade case change char character check collate column
          condition connection constraint continue convert create cross
          current_date current_time current_timestamp current_user cursor
          database databases day_hour day_microsecond day_minute day_second
          dec decimal declare default delayed delete desc describe
          deterministic distinct distinctrow div double drop dual
          each else elseif enclosed escaped exists exit explain
          false fetch float for force foreign from fulltext
          goto grant group
          having high_priority hour_microsecond hour_minute hour_second
          if ignore in index infile inner inout insensitive insert
          int integer interval into is iterate
          join
          key keys kill
          leading leave left like limit lines load localtime
          localtimestamp lock long longblob longtext loop low_priority
          match mediumblob mediumint mediumtext middleint
          minute_microsecond minute_second mod modifies
          natural not no_write_to_binlog null numeric
          on optimize option optionally or order out outer outfile
          precision primary procedure purge
          read reads real references regexp release rename repeat
          replace require restrict return revoke right rlike
          schema schemas second_microsecond select sensitive
          separator set show smallint soname spatial specific sql
          sqlexception sqlstate sqlwarning sql_big_result
          sql_calc_found_rows sql_small_result ssl starting straight_join
          table terminated then tinyblob tinyint tinytext to
          trailing trigger true
          undo union unique unlock unsigned update usage use using
          utc_date utc_time utc_timestamp
          values varbinary varchar varcharacter varying
          when where while with write
          xor
          year_month
          zerofill
  ]

  table = {}
  MYSQL_KEYWORDS.each {|w| table[w] = w; table[w.intern] = w }
  MYSQL_KEYWORD_TABLE = table.freeze

  def escape_mysql_keyword(word)
    return MYSQL_KEYWORD_TABLE[word.downcase] ? "`#{word}`" : word
  end



  module SQLBuilder

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

    def build_select_sql(arg=nil, id=nil)
      sql = "select #{arg || '*'} from #{@_table}"
      sql << @_join if @_join
      if (id)
        sql << " where id = #{quote_value(id)}"
      else
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
      sql = "delete #{@_table}"
      if !id.nil?
        sql << " where id = #{escape(v)}"
      elsif @_where
        sql << ' where ' << @_where
      end
      return sql
    end

    def where(key, val=UNDEFINED)
      _where(key, val, ' and ')
      return 
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

    def _factor(key, val)
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

  end


  module MySQLFeature

    def escape_string(str)
      #return @conn.escape_string(str)
      return str.gsub(/'/, "\\\\\'")
    end

    def quote_keyword(str)
      return MYSQL_KEYWORD_TABLE[str] ? "`#{str}`" : str
    end

  end


  module QueryExecutor

    attr_accessor :conn, :builder, :debug, :table_prefix

    def set_table(table)
      @builder._table = @table_prefix ? @table_prefix + table : table
    end

    def get(table, id=nil)
      set_table(table)
      yield(@builder) if block_given?
      sql = @builder.build_select_sql('*', id)
      result = execute(sql)
      @builder.clear()
      hash = result.fetch_hash()
      result.free()
      return hash
    end

    def get_all(table, id=nil)
      set_table(table)
      yield(@builder) if block_given?
      sql = @builder.build_select_sql('*', id)
      result = execute(sql)
      @builder.clear()
      list = []
      while hash = result.fetch_hash()
        list << hash
      end
      result.free()
      return list
    end

    def select(table, columns=nil, klass=Hash)
      set_table(table)
      yield(@builder) if block_given?
      sql = @builder.build_select_sql(columns, nil)
      result = execute(sql)
      @builder.clear()
      list = []
      if klass == Hash
        result.each_hash {|hash| list << hash }
      elsif klass == Array
        result.each {|arr| list << arr }
      else
        result.each_hash do |hash|
          list << (obj = klass.new)
          #hash.each {|k, v| obj.instance_variable_set("@#{k}", v) }
          hash.each {|k, v| obj.__send__("#{k}=", v) }
        end
      end
      result.free()
      return list
    end

    def select_id(table)
      set_table(table)
      yield(@builder) if block_given?
      sql = @builder.build_select_sql('id', nil)
      result = execute(sql)
      @builder.clear()
      return result.collect {|arr| arr.first }
    end

    def insert(table, values)
      set_table(table)
      sql = @builder.build_insert_sql(values)
      @builder.clear()
      return execute(sql)
    end

    def last_insert_id
      return @conn.insert_id
    end

    def update(table, values, id=nil)
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

    def transaction
      return execute('transaction') unless block_given?
      execute('transaction')
      yield(@builder)
      return commit()
    rescue Exception => ex
      rollback()
      raise ex
    end

    def commit
      return execute('commit')
    end

    def rollback
      return execute('rollback')
    end

  end


  class Query
    include SQLBuilder
    include QueryExecutor
    include MySQLFeature

    def initialize(conn)
      @conn = conn
      @builder = self
    end

  end


end


if __FILE__ == $0
  conn = Mysql.connect('localhost', 'user1', 'passwd1', 'example1')
  #p conn.stat()

  #result = conn.query('select * from stocks');
  #while row = result.fetch_row()
  #while row = result.fetch_hash()
  #  p row
  #end

  #st = conn.prepare('select * from stocks')
  #result = st.execute
  #while row = result.fetch()
  #  p row
  #end

  q = Kwery::Query.new(conn)
  q.debug = true
  #ret = q.get('stocks', 3)
  #ret = q.get('stocks') {|c| c.where('id=4') }
  #ret = q.get_all('stocks') {|c| c.where('symbol !=', 'AAPL').where('`change` >', 0.1) }
  #p ret
  ## select * from groups outer join groups on groups.owner_id = users.id;
  #result = q.execute('select * from groups left outer join users on groups.owner_id = users.id');
  #result = q.execute('select groups.*, users.* from groups left outer join users on groups.owner_id = users.id');
  #result.each_hash {|h| p h }
  #ret = q.select('groups', '*', Array) {|c| c.left_outer_join('users', 'owner_id') }
  #p ret

  q.execute('drop table if exists teams')
  q.execute('drop table if exists members')
  q.commit
  q.execute <<END
create table teams (
  id         integer       primary key auto_increment,
  name       varchar(255)  not null unique,
  `desc`     text,
  owner_id   integer       references members(id),
  created_at timestamp     not null,
  updated_at timestamp     not null
)
END

  q.execute <<END
create table members (
  id         integer       primary key auto_increment,
  name       varchar(255)  not null,
  `desc`     text,
  team_id    integer       references teams(id),
  created_at timestamp     not null,
  updated_at timestamp     not null
)
END

  now = :current_timestamp
  q.insert('teams', {:name=>'sos', :desc=>'SOS Brigate'})
  q.insert('teams', [nil, 'ryouou', 'Ryouou Gakuen High School', nil, now, now])

  q.insert('members', {:name=>'Haruhi', :team_id=>1, :created_at=>now, :updated_at=>now})
  q.insert('members', {:name=>'Mikuru', :team_id=>1, :created_at=>Time.now, :updated_at=>Time.now})
  q.insert('members', {:name=>'Yuki', :team_id=>1, :created_at=>:'now()', :updated_at=>:'now()'})
  q.insert('members', [nil, 'Konata',  nil, 2, now, now])
  q.insert('members', [nil, 'Kagami',  nil, 2, now, now])
  q.insert('members', [nil, 'Tsukasa', nil, 2, now, now])
  q.insert('members', [nil, 'Miyuki',  nil, 2, now, now])

  q.update('teams', {:desc=>'Haruhi Suzumiya', :updated_at=>now}, 1)
  q.update('teams', {:desc=>"Haruhi's brigate", :updated_at=>Time.now}) {|c| c.where('name = ', 'sos') }
  q.update('teams', {:owner_id=>1, :updated_at=>now}) {|c| c.where(:name, 'sos') }
  begin
    q.update('teams', {:owner_id=>1})
  rescue => ex
    p ex
  end

  require 'pp'

  pp q.get('teams', 1)
  puts
  pp q.get_all('teams')
  puts
  pp q.get_all('members') {|c| c.where('team_id = ', 2).order_by_desc(:name) }
  puts
  pp q.select('members, teams', '*', Array) {|c| c.where('members.team_id = teams.id') }

  puts
  pp q.select('teams, members', '*', Array) {|c| c.where('teams.owner_id = members.id') }

  puts
  pp q.select('teams', '*', Array) {|c| c.left_outer_join('members', 'owner_id') }

  conn.close
end

=begin

module ColumnBuilder
  def integer(name, max=nil)
    @_max = max
    yield(self) if block_given?
    @_fields << build_integer_column(
  end
  def build_integer_column
    
  end
end

class TableBuilder
  def initialize(conn)
    @conn = conn
  end
  def create_table(table_name, opts={})
    yield(self)
  end
end

q.create_table('teams') do |t|
  #t.add(:id)
  t.integer(:id) {|f| f.primary_key.auto_increment }
  t.string(:name, 255) {|f| f.not_null.unique }
  t.text(:desc)
  #t.add(:created_at)
  #t.add(:updated_at)
  t.timestamp(:created_at) {|f| f.not_null }
  t.timestamp(:updated_at) {|f| f.not_null.on_update(:current_timestamp) }
  t.boolean(:delete) {|f| f.default(false) }
end

=end
