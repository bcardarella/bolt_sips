defmodule Bolt.Sips.Internals.PackStream.DecoderImplV1 do
  alias Bolt.Sips.Types

  defmacro __using__(_options) do
    quote do
      import unquote(__MODULE__)

      @last_version Bolt.Sips.Internals.BoltVersionHelper.last()

      # Null
      @null_marker 0xC0

      # Boolean
      @true_marker 0xC3
      @false_marker 0xC2

      # String
      @tiny_bitstring_marker 0x8
      @bitstring8_marker 0xD0
      @bitstring16_marker 0xD1
      @bitstring32_marker 0xD2

      # Integer
      @int8_marker 0xC8
      @int16_marker 0xC9
      @int32_marker 0xCA
      @int64_marker 0xCB

      # Float
      @float_marker 0xC1

      # List
      @tiny_list_marker 0x9
      @list8_marker 0xD4
      @list16_marker 0xD5
      @list32_marker 0xD6

      # Map
      @tiny_map_marker 0xA
      @map8_marker 0xD8
      @map16_marker 0xD9
      @map32_marker 0xDA

      # Structure
      @tiny_struct_marker 0xB
      @struct8_marker 0xDC
      @struct16_marker 0xDD

      # Node
      @node_marker 0x4E

      # Relationship
      @relationship_marker 0x52

      # Unbounded relationship
      @unbounded_relationship_marker 0x72

      # Path
      @path_marker 0x50

      @spec decode(binary() | {integer(), binary(), integer()}, integer()) ::
              list() | {:error, :not_implemented}
      def decode(<<@null_marker, rest::binary>>, bolt_version) when bolt_version <= @last_version do
        [nil | decode(rest, bolt_version)]
      end

      # Boolean
      def decode(<<@true_marker, rest::binary>>, bolt_version) when bolt_version <= @last_version do
        [true | decode(rest, bolt_version)]
      end

      def decode(<<@false_marker, rest::binary>>, bolt_version) when bolt_version <= @last_version do
        [false | decode(rest, bolt_version)]
      end

      # Float
      def decode(<<@float_marker, number::float, rest::binary>>, bolt_version)
          when bolt_version <= @last_version do
        [number | decode(rest, bolt_version)]
      end

      # Strings
      def decode(<<@tiny_bitstring_marker::4, str_length::4, rest::bytes>>, bolt_version)
          when bolt_version <= @last_version do
        decode_string(rest, str_length, bolt_version)
      end

      def decode(<<@bitstring8_marker, str_length, rest::bytes>>, bolt_version)
          when bolt_version <= @last_version do
        decode_string(rest, str_length, bolt_version)
      end

      def decode(<<@bitstring16_marker, str_length::16, rest::bytes>>, bolt_version)
          when bolt_version <= @last_version do
        decode_string(rest, str_length, bolt_version)
      end

      def decode(<<@bitstring32_marker, str_length::32, rest::binary>>, bolt_version)
          when bolt_version <= @last_version do
        decode_string(rest, str_length, bolt_version)
      end

      # Lists
      def decode(<<@tiny_list_marker::4, list_size::4>> <> bin, bolt_version)
          when bolt_version <= @last_version do
        decode_list(bin, list_size, bolt_version)
      end

      def decode(<<@list8_marker, list_size::8>> <> bin, bolt_version)
          when bolt_version <= @last_version do
        decode_list(bin, list_size, bolt_version)
      end

      def decode(<<@list16_marker, list_size::16>> <> bin, bolt_version)
          when bolt_version <= @last_version do
        decode_list(bin, list_size, bolt_version)
      end

      def decode(<<@list32_marker, list_size::32>> <> bin, bolt_version)
          when bolt_version <= @last_version do
        decode_list(bin, list_size, bolt_version)
      end

      # Maps
      def decode(<<@tiny_map_marker::4, entries::4>> <> bin, bolt_version)
          when bolt_version <= @last_version do
        decode_map(bin, entries, bolt_version)
      end

      def decode(<<@map8_marker, entries::8>> <> bin, bolt_version)
          when bolt_version <= @last_version do
        decode_map(bin, entries, bolt_version)
      end

      def decode(<<@map16_marker, entries::16>> <> bin, bolt_version)
          when bolt_version <= @last_version do
        decode_map(bin, entries, bolt_version)
      end

      def decode(<<@map32_marker, entries::32>> <> bin, bolt_version)
          when bolt_version <= @last_version do
        decode_map(bin, entries, bolt_version)
      end

      # Struct
      def decode(<<@tiny_struct_marker::4, struct_size::4, sig::8>> <> struct, bolt_version)
          when bolt_version <= @last_version do
        decode({sig, struct, struct_size}, bolt_version)
      end

      def decode(<<@struct8_marker, struct_size::8, sig::8>> <> struct, bolt_version)
          when bolt_version <= @last_version do
        decode({sig, struct, struct_size}, bolt_version)
      end

      def decode(<<@struct16_marker, struct_size::16, sig::8>> <> struct, bolt_version)
          when bolt_version <= @last_version do
        decode({sig, struct, struct_size}, bolt_version)
      end

      ######### SPECIAL STRUCTS

      # Node - handles both old (3 fields) and new (4 fields with element_id) formats
      def decode({@node_marker, struct, struct_size}, bolt_version)
          when bolt_version <= @last_version do
        {decoded, rest} = decode_struct(struct, struct_size, bolt_version)

        node =
          case decoded do
            [id, labels, props, element_id] ->
              %Types.Node{id: id, labels: labels, properties: props, element_id: element_id}

            [id, labels, props] ->
              %Types.Node{id: id, labels: labels, properties: props}
          end

        [node | rest]
      end

      # Relationship - handles both old (5 fields) and new (8 fields with element_ids) formats
      def decode({@relationship_marker, struct, struct_size}, bolt_version)
          when bolt_version <= @last_version do
        {decoded, rest} = decode_struct(struct, struct_size, bolt_version)

        relationship =
          case decoded do
            [id, start_node, end_node, type, props, element_id, start_element_id, end_element_id] ->
              %Types.Relationship{
                id: id,
                start: start_node,
                end: end_node,
                type: type,
                properties: props,
                element_id: element_id,
                start_element_id: start_element_id,
                end_element_id: end_element_id
              }

            [id, start_node, end_node, type, props] ->
              %Types.Relationship{
                id: id,
                start: start_node,
                end: end_node,
                type: type,
                properties: props
              }
          end

        [relationship | rest]
      end

      # UnboundedRelationship - handles both old (3 fields) and new (4 fields with element_id) formats
      def decode({@unbounded_relationship_marker, struct, struct_size}, bolt_version)
          when bolt_version <= @last_version do
        {decoded, rest} = decode_struct(struct, struct_size, bolt_version)

        unbounded_relationship =
          case decoded do
            [id, type, props, element_id] ->
              %Types.UnboundRelationship{
                id: id,
                type: type,
                properties: props,
                element_id: element_id
              }

            [id, type, props] ->
              %Types.UnboundRelationship{
                id: id,
                type: type,
                properties: props
              }
          end

        [unbounded_relationship | rest]
      end

      # Path
      def decode({@path_marker, struct, struct_size}, bolt_version)
          when bolt_version <= @last_version do
        {[nodes, relationships, sequence], rest} =
          decode_struct(struct, struct_size, bolt_version)

        path = %Types.Path{
          nodes: nodes,
          relationships: relationships,
          sequence: sequence
        }

        [path | rest]
      end

      # Manage the end of data
      def decode("", bolt_version) when bolt_version <= @last_version do
        []
      end

      # Integers
      def decode(<<@int8_marker, int::signed-integer, rest::binary>>, bolt_version)
          when bolt_version <= @last_version do
        [int | decode(rest, bolt_version)]
      end

      def decode(<<@int16_marker, int::signed-integer-16, rest::binary>>, bolt_version)
          when bolt_version <= @last_version do
        [int | decode(rest, bolt_version)]
      end

      def decode(<<@int32_marker, int::signed-integer-32, rest::binary>>, bolt_version)
          when bolt_version <= @last_version do
        [int | decode(rest, bolt_version)]
      end

      def decode(<<@int64_marker, int::signed-integer-64, rest::binary>>, bolt_version)
          when bolt_version <= @last_version do
        [int | decode(rest, bolt_version)]
      end

      def decode(<<int::signed-integer, rest::binary>>, bolt_version)
          when bolt_version <= @last_version do
        [int | decode(rest, bolt_version)]
      end
    end
  end
end
