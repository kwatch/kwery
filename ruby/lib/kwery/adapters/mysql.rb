###
### $Rev$
### $Release: $
### $Copyright$
### $License$
###


require 'kwery/query'
require 'kwery/table'
require 'mysql'


## check whether motto_mysql is available or not
arr = [ :fetch_one_hash, :fetch_one_array, :fetch_one_object,
        :fetch_all_hash, :fetch_all_array, :fetch_all_object, ]
HAS_MOTTO = arr.all? {|m| ::Mysql::Result.method_defined?(m) }


module Kwery

  module MySQLCommon

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

    def escape_string(str)
      #return @conn.escape_string(str)
      return str.gsub(/'/, "\\\\\'")
    end

    def quote_keyword(word)
      return MYSQL_KEYWORD_TABLE[word] ? "`#{word}`" : word
    end

  end


  class MySQLQueryContext < QueryContext
    include MySQLCommon

  end


  class MySQLQuery < Query
    include MySQLCommon

    def initialize(*args)
      super
      @auto_free = false
    end if HAS_MOTTO

    def __execute__(sql)
      @output << sql << "\n" if @output
      return @conn.query(sql)
    end if HAS_MOTTO

    def __execute__(sql)
      @output << sql << "\n" if @output
      #return @conn.query(sql)
      stmt = @conn.prepare(sql)
      stmt.execute()
      meta = stmt.result_metadata()
      field_names = []
      if meta
        while field = meta.fetch_field
          #field_names << field.name.intern
          field_names << field.name
        end
        meta.free()
      end
      return MySQLResult.new(field_names, stmt)
    end unless HAS_MOTTO

    def start_transaction   ## prepared statement doesn't support transaction
      return @conn.query('start transaction')
    end unless HAS_MOTTO

    def commit
      return @conn.query('commit')
    end unless HAS_MOTTO

    def rollback
      return @conn.query('rollback')
    end unless HAS_MOTTO

  end


  class MySQLResult   # unused when motto_mysql is available

    def initialize(column_names, stmt)
      @column_names = column_names
      @stmt = stmt
    end

    def fetch_array
      return @stmt.fetch()
    end

    def fetch_hash
      arr = @stmt.fetch()
      return nil unless arr
      hash = {}
      @column_names.zip(arr) {|key, val| hash[key] = val }
      return hash
    end

    def each_array
      while arr = @stmt.fetch()
        yield(arr)
      end
      nil
    end

    def each_hash
      while arr = @stmt.fetch()
        hash = {}
        @column_names.zip(arr) {|key, val| hash[key] = val }
        yield(hash)
      end
      nil
    end

    def free
      @column_names = nil
      @stmt.free_result()
    end

  end


  class Query

    def self.new(*args)
      return MySQLQuery.__new__(*args)  # Query.new returns MySQLQuery object
      #(q = MySQLQuery.allocate()).__send__(:initialize, *args); return q
    end

  end


  class QueryContext

    def self.new(*args)
      return MySQLQueryContext.__new__(*args)  # QueryContext.new returns MySQLQueryContext object
    end

  end


  def self.connect(host=nil, user=nil, passwd=nil, dbname=nil, options=nil)
    return ::Mysql.connect(host||'loalhost', user||'root', passwd, dbname)
  end


  SQL_ERROR_CLASS = ::Mysql::Error
  TIMESTAMP_CLASS = ::Mysql::Time
  DATE_CLASS = ::Mysql::Time


  class MySQLColumn < Column
    include MySQLCommon

    def to_sql()
      sql = super
      if @_type == :timestamp && @_default.nil?
        sql << (@_not_null ? ' default 0' : ' null default null')
      end
      return sql
    end

  end


  class Column

    def self.new(*args)
      return MySQLColumn.__new__(*args)  # Column.new returns MySQLColumn object
    end

  end


  class MySQLTable < Table
    include MySQLCommon
  end


  class Table

    def self.new(*args)
      return MySQLTable.__new__(*args)  # Table.new returns MySQLTable object
    end

  end


end


class ::Mysql::Result  # :nodoc:
  alias _fetch_hash fetch_hash
  alias fetch_hash fetch_one_hash
  alias fetch_array fetch_one_array
  alias each_hash fetch_all_hash
  alias each_array fetch_all_array
end if HAS_MOTTO


class ::Mysql::Result # :nodoc:
  alias each_array  each
  alias fetch_array fetch_row
  ## each_hash and fetch_hash are already defined
end unless HAS_MOTTO
