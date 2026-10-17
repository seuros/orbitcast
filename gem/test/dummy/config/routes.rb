# frozen_string_literal: true

Rails.application.routes.draw do
  root "demo#show"
  get "demo", to: "demo#show", as: :demo
end
