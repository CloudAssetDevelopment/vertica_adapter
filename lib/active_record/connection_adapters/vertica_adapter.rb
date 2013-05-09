require 'active_record/connection_adapters/abstract_adapter'
require 'arel/visitors/bind_visitor'

module ActiveRecord
  class Base
    # Establishes a connection to the database that's used by all Active Record objects
    def self.vertica_connection(config) # :nodoc:
      unless defined? Vertica
        begin
          require 'vertica'
        rescue LoadError
          raise "!!! Missing the vertica gem. Add it to your Gemfile: gem 'vertica'"
        end
      end

      config = config.symbolize_keys
      host     = config[:host]
      port     = config[:port] || 5433
      username = config[:username].to_s if config[:username]
      password = config[:password].to_s if config[:password]

      if config.has_key?(:database)
        database = config[:database]
      else
        raise ArgumentError, "No database specified. Missing argument: database."
      end
      if config.has_key?(:schema)
        schema = config[:schema]
      else
        raise ArgumentError, "No database specified. Missing argument: schema."
      end
      conn = Vertica.connect({ :user => username, :password => password, :host => host, :port => port, :database => database , :schema => schema})
      ConnectionAdapters::Vertica.new(conn)
    end

    # def self.instantiate(record)
    #   record.stringify_keys!

    #   sti_class = find_sti_class(record[inheritance_column])
    #   record_id = sti_class.primary_key && record[sti_class.primary_key]
    #   if ActiveRecord::IdentityMap.enabled? && record_id
    #     if (column = sti_class.columns_hash[sti_class.primary_key]) && column.number?
    #       record_id = record_id.to_i
    #     end
    #     if instance = IdentityMap.get(sti_class, record_id)
    #       instance.reinit_with('attributes' => record)
    #     else
    #      instance = sti_class.allocate.init_with('attributes' => record)
    #       IdentityMap.add(instance)
    #     end
    #   else
    #     instance = sti_class.allocate.init_with('attributes' => record)
    #   end

    #   instance
    # end
  end

  module ConnectionAdapters
    class VerticaColumn < Column
      def extract_default(default)
        # blank string or 'NULL' represents NULL
        if default.blank? || default == 'NULL'
          nil
        else
          # if type is string, vertica sends the default wrapped in single quotes
          default = default[1..-2] if type == :string
          super
        end
      end
    end

    class BindSubstitution < Arel::Visitors::MySQL
      include Arel::Visitors::BindVisitor
    end

    class Vertica < AbstractAdapter
      def supports_explain?
        false
      end

      def explain(*args)
        "EXPLAIN not supported in Vertica"
      end

      def initialize(*args)
        super(*args)
        # @visitor = Arel::Visitors::MySQL.new self
        @visitor = BindSubstitution.new self
      end

      def adapter_name #:nodoc:
        'Vertica'.freeze
      end

      def active?
        @connection.opened?
      end

      # Disconnects from the database if already connected, and establishes a
      # new connection with the database.
      def reconnect!
        @connection.reset_connection
      end
      def reset
        reconnect!
      end

      # Close the connection.
      def disconnect!
        @connection.close rescue nil
      end

      # return raw object
      def execute(sql, name=nil)
        log(sql,name) do
          if block_given?
            @connection.query(sql) {|row| yield row }
          else
            @connection.query(sql)
          end
        end
      end

      def exec_query(sql, name = 'SQL', binds = [])
        result = execute(sql, name)
        ActiveRecord::Result.new(result.columns.map(&:name), result.rows.map(&:values))
      end

      def exec_update(sql, name = 'SQL', binds = [])
        if sql =~ /(.+)\s+ORDER\s+BY\s+[^\s](\s+(ASC|DESC))?/i
          super $1, name, binds
        else
          super
        end
      end

      def schema_name
        @schema ||= @connection.options[:schema]
      end

      def tables(name = nil) #:nodoc:
        sql = "SELECT * FROM tables WHERE table_schema = '#{schema_name}'"

        tables = []
        execute(sql, name) { |field| tables << field[:table_name] }
        tables
      end

      def columns(table_name, name = nil)#:nodoc:
        sql = "SELECT * FROM columns WHERE table_name = '#{table_name}'"

        columns = []
        execute(sql, name){ |field| columns << VerticaColumn.new(field[:column_name],field[:column_default],field[:data_type],field[:is_nullable])}
        columns
      end

      def select(sql, name = nil, binds = [])
        rows = []
        execute(sql, name) do |row|
          rows << row.stringify_keys
        end
        rows
      end

      def primary_key(table)
        'id'
      end

      def begin_db_transaction
        execute "BEGIN"
      end

      def commit_db_transaction #:nodoc:
        execute "COMMIT"
      end

      def rollback_db_transaction #:nodoc:
        execute "ROLLBACK"
      end

      def create_savepoint
        execute("SAVEPOINT #{current_savepoint_name}")
      end

      def rollback_to_savepoint
        execute("ROLLBACK TO SAVEPOINT #{current_savepoint_name}")
      end

      def release_savepoint
        execute("RELEASE SAVEPOINT #{current_savepoint_name}")
      end

      ## QUOTING
      def quote_column_name(name) #:nodoc:
        "#{name}"
      end

      def quote_table_name(name) #:nodoc:
        # if schema_name.blank?
          name
        # else
        #   "#{schema_name}.#{name}"
        # end
      end

      def quoted_true
        "1"
      end

      def quoted_false
        "0"
      end

      def table_definition
        TableDefinition.new self
      end

      def add_index(table_name, column_name, options = {})
        #noop
      end

      def remove_index(table_name, options = {})
        #noop
      end

      def rename_index(table_name, old_name, new_name)
        #noop
      end

      def select_rows(sql, name = nil)
        select_raw(sql, name).last
      end

      def select_raw(sql, name = nil)
        res = execute(sql, name)
        return res.columns.collect{|c| c.name}, res.rows
      end

      def last_inserted_id(result)
        @connection.query('select last_insert_id()').rows[0][:last_insert_id]
      end

      class TableDefinition < ActiveRecord::ConnectionAdapters::TableDefinition
        def primary_key(name)
          column(name, 'auto_increment primary key')
        end

        def string(name, opts = {})
          if opts[:limit]
            column(name, "varchar(#{opts[:limit]})")
          else
            column(name, 'varchar')
          end
        end

        def text(name)
          column(name, 'varchar')
        end
      end

    end
  end
end
