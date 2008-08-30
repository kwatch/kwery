#!/usr/bin/ruby

###
### $Rev: 68 $
### $Release: $
### $Copyright$
### $License$
###

require 'rubygems'

spec = Gem::Specification.new do |s|
  ## package information
  s.name        = "kwery"
  s.author      = "makoto kuwata"
  s.email       = "kwa(at)kuwata-lab.com"
  s.rubyforge_project = "kwery"
  s.version     = "$Release$"
  s.platform    = Gem::Platform::RUBY
  s.homepage    = "http://www.kuwata-lab.com/kwery/"
  s.summary     = "a pretty database library, including simple O/R Mapper"
  s.description = <<-'END'
Kwery is a pretty database library, including simple O/R Mapper.
* Small and lightweight (suitable especially for CGI)
* Not necessary to define model class (Hash is used instead)
* You can define model class to map tables (optional).
  END

  ## files
  files = []
  files += Dir.glob('lib/**/*')
  files += Dir.glob('spec/**/*')
  files += Dir.glob('doc/**/*')
  files += %w[README.txt CHANGES.txt MIT-LICENSE setup.rb kwery.gemspec]
  files += Dir.glob('doc-api/**/*')
  s.files       = files
  #s.test_file   = 'spec/spec_all.rb'
end

# Quick fix for Ruby 1.8.3 / YAML bug   (thanks to Ross Bamford)
if (RUBY_VERSION == '1.8.3')
  def spec.to_yaml
    out = super
    out = '--- ' + out unless out =~ /^---/
    out
  end
end

if $0 == __FILE__
  Gem::manage_gems
  Gem::Builder.new(spec).build
end
