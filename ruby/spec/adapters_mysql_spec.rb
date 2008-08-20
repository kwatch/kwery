###
### $Rev$
### $Release: $
### $Copyright$
### $License$
###

File.class_eval do
  path = join(dirname(dirname(expand_path(__FILE__))), 'lib')
  $:.unshift path
  path = join(dirname(dirname(expand_path(__FILE__))), 'spec')
  $:.unshift path
end


#require 'rubygems'
#require 'rspec'
require 'kwery'
require 'kwery/adapters/mysql'
require 'database_config'


conn = Kwery.connect(HOST, USER, PASS, DBNAME)
q = Kwery::Query.new(conn)
#q.debug = true
now = :current_timestamp


describe 'Kwery::MySQLQuery#execute' do

  it "can create table statement." do
    proc {
      q.execute "drop table if exists dummy1"
      q.execute <<END
create table dummy1 (
  id          integer        primary key auto_increment,
  name        varchar(255)   not null unique,
  `desc`      text           not null,
  factor      float          not null,
  birth       date           not null,
  created_at  timestamp      ,
  deleted     boolean        default false
)
END
    }.should_not raise_error(Exception)
  end

  it "can insert data" do
    q.insert('dummy1', [nil, 'Foo', 'foo', 1.23, '2000-01-23', '2008-02-03 12:34:56', false])
    q.insert('dummy1', :name=>'Bar', :desc=>'bar', :factor=>0.5, :birth=>'1999-12-31', :deleted=>false)
    q.select('dummy1', 'count(*) count').first['count'].should == 2
  end

  it "returns non-string data when 'select' statement" do
    hash = q.get('dummy1', 1)
    hash['id'].should be_a_kind_of(Integer)
    hash['name'].should be_a_kind_of(String)
    hash['desc'].should be_a_kind_of(String)
    hash['factor'].should be_a_kind_of(Float)
    hash['birth'].should be_a_kind_of(::Mysql::Time)
    hash['created_at'].should be_a_kind_of(::Mysql::Time)
    hash['deleted'].should be_a_kind_of(Fixnum)
    #
    hash['factor'].should > 1.22
    hash['factor'].should < 1.24
    hash['birth'].year.should == 2000
    hash['birth'].month.should == 1
    hash['birth'].day.should == 23
    hash['birth'].hour.should == 0
    hash['birth'].minute.should == 0
    hash['birth'].second.should == 0
    hash['created_at'].year.should == 2008
    hash['created_at'].month.should == 2
    hash['created_at'].day.should == 3
    hash['created_at'].hour.should == 12
    hash['created_at'].minute.should == 34
    hash['created_at'].second.should == 56
    hash['deleted'].should == 0
  end

  it "*" do
    q.execute('drop table dummy1')
    conn.close()
  end


end


describe "Kwery::Query.new" do

  it "returns Kwery::MySQLQuery object" do
    Kwery::Query.new(nil).should be_a_kind_of(Kwery::MySQLQuery)
  end

end


describe "Kwery::Column.new" do

  it "returns Kwery::MySQLColumn object" do
    Kwery::Column.new('name', :integer, nil, nil).should be_a_kind_of(Kwery::MySQLColumn)
  end

end


describe "Kwery::MySQLColumn.to_sql" do

  it "appends 'null default null' when nullable and no default value" do
    class Foo1
      include Kwery::Model
      create_table("foo1") do |t|
        t.timestamp(:start_at)
      end
    end
    expected =  "create table foo1 (\n"
    expected << "  start_at           timestamp       null default null\n"
    expected << ")"
    Foo1.to_sql.should == expected
  end

  it "appends 'default 0' when not null and no default value" do
    class Foo2
      include Kwery::Model
      create_table("foo2") do |t|
        t.timestamp(:stop_at) {|c| c.not_null }
      end
    end
    expected =  "create table foo2 (\n"
    expected << "  stop_at            timestamp       not null default 0\n"
    expected << ")"
    Foo2.to_sql.should == expected
  end

end
