= Kwery README

Release:: $Release$

Kwery is a pretty database library.
You can use Kwery as database library (like DBI), or as O/R Mapper.


== Features

* Kwery is very small and lightweight.
  It is suitable especially for CGI script.
* It is not necessary to define model class.
  If you don't define model class, you can use Hash object instead of model object.
* You can define model class to map tables (optional).
* It is very easy to use Kwery if you already know SQL.
* Currently Kwery supports only MySQL, but it will be easy to support other RDBMS.
* Kwery doesn't have validation functionality currently.


== Example

	require 'kwery'
	require 'kwery/adapters/mysql'
	conn = Kwery.connect('localhost', 'username', 'password', 'dbname')
	q = Kwery::Qwery.new(conn)
	q.output = $stderr  # for debug
	
	## select * from teams where id = 123
	team = q.get('teams') {|c| c.where(:id, 123) }
			    # or q.get('teams', :id, 123)
	
	## select * from members
	## where team_id = team['id'] and created_at > '2008-01-01'
	## order by name desc
	t = Time.mktime(2008, 1, 1)
	members = q.get_all('members') {|c|
	  c.where(:team_id, team['id']).where('created_at >', t).order_by_desc(:name)
	}
	
	## insert into teams(name, desc) values('sos', 'SOS Brigade')
	q.insert('teams', {:name=>'sos', :desc=>'SOS Brigade'})
	
	## update teams set name='sos', `desc`='SOS Brigade' where id = 123
	values = {:name=>'sos', :desc=>'SOS Brigade'}
	q.update('teams', values) {|c| c.where(:id, 123) }
	                    # or q.update('teams', values, :id, 123)
	
	## delete from members where name == 'taniguchi'
	q.delete('members') {|c| c.where(:name, 'taniguchi') }
	                    # or q.delete('teams', :name, 'taniguchi')

See 'doc/users-guide.html' for details.


== Install

You can install Kwery by RubyGems.

	$ sudo gem install kwery

Or you can install Kwery manually.

	$ tar xzf kwery-$Release$.tar.gz
	$ cd kwery-$Release$/
	$ sudo ruby setup.rb

If you want to use Kwery in your CGI script, it is strongly recommended
to install *without* gems, because gem is too slow for CGI application.


== License

MIT License


== Author, Copyright

makoto kuwata <kwa.at.kuwata-lab.com>

$Copyright$
