require 'flipper/adapters/s3'
require 'flipper/spec/shared_adapter_specs'

RSpec.describe Flipper::Adapters::S3 do
  subject { described_class.new bucket: "estellep-flippertest", prefix: "squishbeans/" }

  before(:each) do
    subject.flush
  end

  it_should_behave_like 'a flipper adapter'
end
