require 'flipper'
require 'aws-sdk'
require 'set'
require 'json'

module Flipper
  module Adapters
    class S3
      include ::Flipper::Adapter

      attr_reader :name

      def initialize(bucket:, prefix: "", client: Aws::S3::Client.new)
        @name = :s3
        @bucket = bucket
        @prefix = prefix
        @client = client

        # Sanity check to see if the bucket exists.
        @client.head_bucket({
          bucket: @bucket
        })
      end

      # Public: Gets the feature keys in the bucket.
      #
      # Returns a Set of Flipper::Feature#key.
      def features
        @client.list_objects_v2({
          bucket: @bucket,
          prefix: @prefix,
        }).contents.map(&:key).map(&method(:feature_key)).to_set
      end

      # Public: Gets the values for all gates for a given feature.
      #
      # Returns a Hash of Flipper::Gate#key => value.
      def get(feature)
        param = Marshal.load(@client.get_object({
          bucket: @bucket,
          key: storage_key(feature.key)
        }).body.read)
      rescue
        param = {}
      ensure 
        return default_config.merge(param)
      end

      # Public: Puts a blob of data into a given storage key.
      def put(feature, blob, overwrite = true)
        @client.put_object({
          bucket: @bucket,
          key: storage_key(feature.key),
          body: Marshal.dump(blob)
        })
      end

      # Public: Removes a feature from the set of known features and clears
      # all the values for the feature.
      def remove(feature)
        @client.delete_object({
          bucket: @bucket,
          key: storage_key(feature.key)
        })
        true
      end

      # Public: Adds a feature to the set of known features.
      def add(feature)
        put(feature, default_config, false)
        true
      end

      # Public: Clears all the gate values for a feature.
      def clear(feature)
        put(feature, default_config)
        true
      end

      # Public: Enables a gate for a given thing.
      #
      # feature - The Flipper::Feature for the gate.
      # gate - The Flipper::Gate to disable.
      # thing - The Flipper::Type being enabled for the gate.
      #
      # Returns true.
      def enable(feature, gate, thing)
        case gate.data_type
        when :boolean
          clear(feature)
          result = get(feature)
          result[gate.key] = thing.value.to_s
          put(feature, result)
        when :integer
          result = get(feature)
          result[gate.key] = thing.value.to_s
          put(feature, result)
        when :set
          result = get(feature)
          result[gate.key] << thing.value.to_s
          put(feature, result)
        end
        true
      end

      # Public: Disables a gate for a given thing.
      #
      # feature - The Flipper::Feature for the gate.
      # gate - The Flipper::Gate to disable.
      # thing - The Flipper::Type being disabled for the gate.
      #
      # Returns true.
      def disable(feature, gate, thing)
        case gate.data_type
        when :boolean
          clear(feature)
        when :integer
          result = get(feature)
          result[gate.key] = thing.value.to_s
          put(feature, result)
        when :set
          result = get(feature)
          result[gate.key] = result[gate.key].delete(thing.value.to_s)
          put(feature, result)
        end
        true
      end

      def flush
        features.each do |feature_key|
          remove Flipper::Feature.new(feature_key, self)
        end
        true
      end

      private

      def storage_key(feature_key)
        "#{@prefix}#{feature_key}"
      end 

      def feature_key(storage_key)
        storage_key[@prefix.length..-1]
      end

    end
  end
end
