###
### $Rev$
### $Release: $
### $Copyright$
### $License$
###


###
### simple database access library
###
module Kwery


  ## represents dummy data
  module UNDEFINED
  end


  class Error < StandardError
  end


end

require 'kwery/helper'
require 'kwery/query'
require 'kwery/model'
