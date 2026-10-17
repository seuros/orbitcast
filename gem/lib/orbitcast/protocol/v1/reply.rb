# frozen_string_literal: true

require_relative "../v1"

module Orbitcast
  module Protocol
    module V1
      # Rails' answer to one request, built fail-closed.
      #
      # The builder is the only place a reply is constructed, because the rules
      # below are the ones that keep a half-finished Rails request from widening
      # access:
      #
      #   * `denied` and `fault` carry no effects, no state replacement, and no
      #     identity. They are a decision (or a failure), never a state change.
      #     Anything the callbacks collected before the refusal is dropped here,
      #     not filtered by the caller.
      #   * An unchanged state is *omitted*. Absent and `null` both mean
      #     unchanged, `{}` means clear — so a reply that emitted `{}` for "I
      #     did not touch it" would silently wipe the session's declared state.
      #   * `identity` belongs to `identify` only, and an empty map there is a
      #     valid accepted anonymous connection.
      #   * `new_revision` never moves backwards: accepted stateful operations
      #     advance it by one, everything else echoes `expected_revision`.
      #
      # `validate!` runs the same limit checks Rust runs on receipt, so an
      # envelope the relay would reject never leaves this process.
      class Reply
        UNCHANGED = :unchanged

        class << self
          def accepted(request, effects: [], state: UNCHANGED, subscription_state: UNCHANGED,
                       identity: nil, describe: nil)
            new(
              request: request,
              outcome: OUTCOME_ACCEPTED,
              effects: effects,
              state: state,
              subscription_state: subscription_state,
              identity: identity,
              describe: describe
            )
          end

          def denied(request, reason)
            new(request: request, outcome: OUTCOME_DENIED, reason: reason)
          end

          def fault(request, reason)
            new(request: request, outcome: OUTCOME_FAULT, reason: reason)
          end

          # The capability handshake. Not stateful: the revision is echoed.
          def describe(request, supported_versions: [VERSION], software_version: nil,
                       capabilities: [], ready: true)
            accepted(
              request,
              describe: {
                "supported_versions" => supported_versions,
                "software_version" => software_version.to_s,
                "capabilities" => capabilities,
                "ready" => ready
              }
            )
          end
        end

        def initialize(request:, outcome:, effects: [], state: UNCHANGED,
                       subscription_state: UNCHANGED, identity: nil, reason: nil, describe: nil)
          unless OUTCOMES.include?(outcome)
            raise ProtocolError.new("unknown outcome #{outcome.inspect}", field: "outcome")
          end

          @operation = request.operation
          @request_id = request.request_id
          @sequence = request.sequence
          @expected_revision = request.expected_revision
          @outcome = outcome
          @reason = reason.nil? ? nil : truncate_reason(reason.to_s)
          @describe = describe

          if accepted?
            @effects = effects.respond_to?(:to_a) ? effects.to_a : Array(effects)
            @state = state
            @subscription_state = subscription_state
            @identity = identity
          else
            # Fail closed: a refusal or a failure changes nothing, whatever the
            # callbacks managed to collect before it.
            @effects = [].freeze
            @state = UNCHANGED
            @subscription_state = UNCHANGED
            @identity = nil
          end
        end

        attr_reader :operation, :outcome, :request_id, :sequence, :effects, :reason, :describe

        def accepted?
          @outcome == OUTCOME_ACCEPTED
        end

        def denied?
          @outcome == OUTCOME_DENIED
        end

        def fault?
          @outcome == OUTCOME_FAULT
        end

        # Accepted stateful operations advance the revision; `describe` and any
        # non-acceptance echo what the relay believed.
        def new_revision
          return @expected_revision unless accepted? && V1.stateful?(operation)

          @expected_revision + 1
        end

        def state
          @state == UNCHANGED ? nil : @state
        end

        def subscription_state
          @subscription_state == UNCHANGED ? nil : @subscription_state
        end

        def identity
          @identity
        end

        def to_h
          validate!

          hash = {
            "version" => VERSION,
            "operation" => operation,
            "outcome" => outcome,
            "request_id" => request_id,
            "sequence" => sequence,
            "new_revision" => new_revision
          }

          # Absent means unchanged. Never `null`, never `{}` for "untouched".
          hash["state"] = @state unless @state == UNCHANGED
          hash["subscription_state"] = @subscription_state unless @subscription_state == UNCHANGED
          hash["identity"] = @identity || {} if emits_identity?
          hash["effects"] = @effects
          hash["reason"] = @reason unless @reason.nil?
          hash["describe"] = @describe unless @describe.nil?
          hash
        end

        def to_json(*args)
          JSON.generate(to_h, *args)
        end

        # The same checks Rust runs on receipt.
        def validate!
          V1.validate_uuid!("request_id", request_id)

          unless @reason.nil?
            V1.validate_bounded!("reason", @reason, MAX_REASON_LEN)
          end

          unless accepted?
            raise_scope("effects") unless @effects.empty?
            raise_scope("state") unless @state == UNCHANGED
            raise_scope("subscription_state") unless @subscription_state == UNCHANGED
            raise_scope("identity") unless @identity.nil? || @identity.empty?
          end

          if @effects.size > MAX_EFFECTS
            raise ProtocolError.new(
              "one reply may carry at most #{MAX_EFFECTS} effects", field: "effects"
            )
          end
          @effects.each { |effect| validate_effect!(effect) }

          V1.validate_state_map!("identity", @identity) unless @identity.nil?
          V1.validate_state_map!("state", @state) unless @state == UNCHANGED
          unless @subscription_state == UNCHANGED
            V1.validate_state_map!("subscription_state", @subscription_state)
          end

          validate_describe! if operation == OPERATION_DESCRIBE && accepted?
          self
        end

        private

        def emits_identity?
          accepted? && operation == OPERATION_IDENTIFY
        end

        def raise_scope(field)
          raise ProtocolError.new("#{field} is not allowed on a #{outcome} reply", field: field)
        end

        def validate_effect!(effect)
          kind = effect["kind"]
          unless EFFECT_KINDS.include?(kind)
            raise ProtocolError.new("unknown effect kind #{kind.inspect}", field: "kind")
          end

          case kind
          when EFFECT_ADD_STREAM, EFFECT_REMOVE_STREAM
            V1.validate_identifier!(effect["identifier"])
            V1.validate_stream!(effect["stream"])
          when EFFECT_REMOVE_ALL_STREAMS
            V1.validate_identifier!(effect["identifier"])
          when EFFECT_TRANSMIT
            V1.validate_identifier!(effect["identifier"])
            V1.validate_json_text!("json_payload", effect["json_payload"])
          when EFFECT_CLOSE
            V1.validate_close_reason!(effect["reason"])
          when EFFECT_PRESENCE_GRANT
            V1.validate_identifier!(effect["identifier"])
            V1.validate_stream!(effect["stream"])
            V1.validate_bounded!("subject_id", effect["subject_id"], MAX_SUBJECT_ID_LEN)
            unless effect["ttl_ms"].is_a?(Integer) && effect["ttl_ms"].positive?
              raise ProtocolError.new("ttl_ms must be greater than zero", field: "ttl_ms")
            end
          when EFFECT_PRESENCE_REVOKE
            V1.validate_identifier!(effect["identifier"])
            V1.validate_stream!(effect["stream"])
            V1.validate_bounded!("subject_id", effect["subject_id"], MAX_SUBJECT_ID_LEN)
          end
        end

        def validate_describe!
          unless @describe.is_a?(Hash)
            raise ProtocolError.new("an accepted describe requires a describe payload",
                                    field: "describe")
          end

          versions = @describe["supported_versions"]
          unless versions.is_a?(Array) && versions.include?(VERSION)
            raise ProtocolError.new("describe must advertise version #{VERSION}",
                                    field: "supported_versions")
          end

          capabilities = @describe["capabilities"] || []
          if capabilities.size > MAX_CAPABILITIES
            raise ProtocolError.new(
              "describe may advertise at most #{MAX_CAPABILITIES} capabilities",
              field: "capabilities"
            )
          end
          capabilities.each { |capability| V1.validate_bounded!("capability", capability, MAX_LABEL_LEN) }

          software_version = @describe["software_version"].to_s
          unless software_version.empty?
            V1.validate_bounded!("software_version", software_version, MAX_LABEL_LEN)
          end
        end

        # The reason is a diagnostic for relay logs; it is never shown to a
        # browser, and it is bounded so a stack-trace-sized string cannot make
        # the reply unparseable.
        def truncate_reason(reason)
          return reason if reason.bytesize <= MAX_REASON_LEN

          reason.byteslice(0, MAX_REASON_LEN).scrub("")
        end
      end
    end
  end
end
