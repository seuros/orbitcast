# frozen_string_literal: true

# The dummy application's only model. It exists so the Rails-dispatch
# integration test can prove that declared state and identity round-trip as
# GlobalID references through a real ActiveRecord object — and that the record
# being deleted between two requests restores as nil rather than raising.
#
# The signing-fixture exporters never touch it: nothing connects at boot.
class Widget < ActiveRecord::Base
end
