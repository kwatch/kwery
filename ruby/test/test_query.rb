###
### $Rev$
### $Release: $
### $Copyright$
### $License$
###

p File.join(File.dirname(File.dirname(File.expand_path(__FILE__))), 'lib')
$:.unshift File.join(File.dirname(File.dirname(File.expand_path(__FILE__))), 'lib')


#require 'rubygems'
#require 'rspec'
require 'kwery'
require 'mysql'


HOST = 'localhost'
USER = 'user1'
PASS = 'passwd1'
DBNAME = 'example1'


conn = Mysql.connect(HOST, USER, PASS, DBNAME)
q = Kwery::Query.new(conn)
#q.debug = true
now = :current_timestamp


describe 'Kwery::Query#execute' do

  it "should execute create table sql statement." do
    q.execute "drop table if exists teams"
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
    q.execute "drop table if exists members"
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

  end

end


describe 'Kwery::Query#insert' do

  it "is able to insert hash data" do
    q.insert('teams', {:name=>'sos', :desc=>'SOS Brigate'})
    id = q.last_insert_id
    #
    q.insert('members', {:name=>'Haruhi', :team_id=>id, :created_at=>now, :updated_at=>now})
    q.insert('members', {:name=>'Mikuru', :team_id=>id, :created_at=>Time.now, :updated_at=>Time.now})
    q.insert('members', {:name=>'Yuki', :team_id=>id, :created_at=>:'now()', :updated_at=>:'now()'})
    #
    q.select('teams').length.should == 1
    q.select('members').length.should == 3
  end

  it "is able to insert array data" do
    q.insert('teams', [nil, 'ryouou', 'Ryouou Gakuen High School', nil, now, now])
    id = q.last_insert_id
    #
    q.insert('members', [nil, 'Konata',  nil, id, now, now])
    q.insert('members', [nil, 'Kagami',  nil, id, now, now])
    q.insert('members', [nil, 'Tsukasa', nil, id, now, now])
    q.insert('members', [nil, 'Miyuki',  nil, id, now, now])
    #
    q.select('teams').length.should == 2
    q.select('members').length.should == 7
  end

end


describe 'Kwery::Query#update' do

  it "is able to update data with conditions" do
    desc = "Haruhi's brigate"  # contains "'"
    q.update('teams', {:desc=>desc, :updated_at=>Time.now}) {|c| c.where('name = ', 'sos') }
    hash = q.get('teams', 1)
    hash['desc'].should == desc
  end

  it "is able to update data with id" do
    mikuru = q.get('members') {|c| c.where(:name, 'Mikuru') }
    mikuru['desc'].should == nil
    desc = 'Future-woman'
    q.update('members', {:desc=>desc, :updated_at=>now}, mikuru['id'])
    mikuru = q.get('members', mikuru['id'].to_i)
    mikuru['desc'].should == desc
  end

  it "should throw ArugmentError when condition is not specified" do
    ex = nil
    proc {
      begin
        q.update('teams', {:owner_id=>1})
      rescue Exception => ex
        raise ex
      end
    }.should raise_error(ArgumentError)
    ex.message.should == 'update condition is reqiured.'
  end

end
