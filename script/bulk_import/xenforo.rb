# frozen_string_literal: true

require_relative "base"
require "set"
require "mysql2"
require "htmlentities"

class BulkImport::XenForo < BulkImport::Base

  TABLE_PREFIX = "xf_"
  SUSPENDED_TILL ||= Date.new(3000, 1, 1)
  ATTACHMENT_DIR ||= ENV['ATTACHMENT_DIR'] || '/shared/import/data/attachments'
  AVATAR_DIR ||= ENV['AVATAR_DIR'] || '/shared/import/data/avatars'
  ATTACHMENT_IMPORTERS = 4

  def initialize
    super

    host     = ENV["DB_HOST"] || "172.17.0.1"
    username = ENV["DB_USERNAME"] || "rpn_user"
    password = ENV["DB_PASSWORD"]
    database = ENV["DB_NAME"] || "rpnation_xf"
    charset  = ENV["DB_CHARSET"] || "utf8"

    @html_entities = HTMLEntities.new
    @encoding = CHARSET_MAP[charset]

    @client = Mysql2::Client.new(
      host: host,
      username: username,
      password: password,
      database: database,
      encoding: charset,
      reconnect: true
    )

    @client.query_options.merge!(as: :array, cache_rows: false)

  end

  def execute
    # enable as per requirement:
    SiteSetting.automatic_backups_enabled = false
    SiteSetting.disable_emails = "non-staff"
    SiteSetting.authorized_extensions = '*'
    # SiteSetting.max_image_size_kb = 102400
    # SiteSetting.max_attachment_size_kb = 102400
    # SiteSetting.clean_up_uploads = false
    # SiteSetting.clean_orphan_uploads_grace_period_hours = 43200
    SiteSetting.tagging_enabled = true
    SiteSetting.max_tags_per_topic = 10
    SiteSetting.max_tag_length = 100

    #import_groups
    import_users
    #import_group_users

    import_user_emails
    import_user_stats

    import_user_profiles

    import_categories
    import_topics
    import_posts

    import_topic_tags

    import_private_topics
    import_topic_allowed_users
    import_private_posts

    import_likes

    # Discard massive tracking arrays
    # These only take up memory at this point

    @topics = {}
    @posts = {}
    @private_topics = {}
    @private_posts = {}
    @post_number_by_post_id = {}
    @topic_id_by_post_id = {}

    #create_permalink_file
    import_attachments
    import_avatars
  end

  def import_groups
    puts "Importing groups..."

    groups = mysql_stream <<-SQL
        SELECT usergroupid, title, description, usertitle
          FROM #{TABLE_PREFIX}usergroup
         WHERE usergroupid > #{@last_imported_group_id}
      ORDER BY usergroupid
    SQL

    create_groups(groups) do |row|
      {
        imported_id: row[0],
        name: normalize_text(row[1]),
        bio_raw: normalize_text(row[2]),
        title: normalize_text(row[3]),
      }
    end
  end

  def import_users
    puts "Importing users..."

    users = mysql_stream <<-SQL
        SELECT u.user_id, u.username, u.email, u.register_date, up.dob_year, up.dob_month, up.dob_day, u.last_activity, u.custom_title, u.is_moderator, u.is_staff, u.is_admin, u.is_banned, ub.ban_date, ub.end_date
          FROM #{TABLE_PREFIX}user u
     LEFT JOIN #{TABLE_PREFIX}user_ban ub ON ub.user_id = u.user_id
     LEFT JOIN #{TABLE_PREFIX}user_profile up ON u.user_id = up.user_id
         WHERE u.user_id > #{@last_imported_user_id}
      ORDER BY u.user_id
    SQL

    create_users(users) do |row|
      birthday = Date.parse("#{row[4]}-#{row[5]}-#{row[6]}") rescue nil
      u = {
        imported_id: row[0],
        username: normalize_text(row[1].gsub(/\s+/, "")),
        name: normalize_text(row[1]),
        created_at: Time.zone.at(row[3]),
        date_of_birth: birthday,
        last_seen_at: row[7],
        title: row[8],
        moderator: row[9] == 1 || row[10] == 1,
        admin: row[11] == 1,
      }
      if row[12] == 1
        u[:suspended_at] = Time.zone.at(row[13])
        u[:suspended_till] = row[14] > 0 ? Time.zone.at(row[14]) : SUSPENDED_TILL
      end
      u
    end
  end

  def import_user_emails
    puts "Importing user emails..."

    users = mysql_stream <<-SQL
        SELECT u.user_id, email, register_date
          FROM #{TABLE_PREFIX}user u
         WHERE u.user_id > #{@last_imported_user_id}
      ORDER BY u.user_id
    SQL

    create_user_emails(users) do |row|
      {
        imported_id: row[0],
        imported_user_id: row[0],
        email: row[1],
        created_at: Time.zone.at(row[2])
      }
    end
  end

  def import_user_stats
    puts "Importing user stats..."

    users = mysql_stream <<-SQL
              SELECT u.user_id, register_date, message_count, COUNT(DISTINCT t.thread_id) AS threads, reaction_score AS likes, COUNT(DISTINCT rg.reaction_content_id) AS likes_given
                FROM #{TABLE_PREFIX}user u
     LEFT JOIN #{TABLE_PREFIX}thread t ON u.user_id = t.user_id
     LEFT JOIN #{TABLE_PREFIX}reaction_content rg ON u.user_id = rg.reaction_user_id
               WHERE u.user_id > #{@last_imported_user_id}
            GROUP BY u.user_id
            ORDER BY u.user_id
    SQL

    create_user_stats(users) do |row|
      user = {
        imported_id: row[0],
        imported_user_id: row[0],
        new_since: Time.zone.at(row[1]),
        post_count: row[2],
        topic_count: row[3],
        likes_given: row[5],
        likes_received: row[4],
      }
      user
    end
  end

  def import_group_users
    puts "Importing group users..."

    group_users = mysql_stream <<-SQL
      SELECT usergroupid, userid
        FROM #{TABLE_PREFIX}user
       WHERE userid > #{@last_imported_user_id}
    SQL

    create_group_users(group_users) do |row|
      {
        group_id: group_id_from_imported_id(row[0]),
        user_id: user_id_from_imported_id(row[1]),
      }
    end
  end

  def import_user_profiles
    puts "Importing user profiles..."

    user_profiles = mysql_stream <<-SQL
        SELECT user_id, website, about
          FROM #{TABLE_PREFIX}user_profile
         WHERE user_id > #{@last_imported_user_id}
      ORDER BY user_id
    SQL

    create_user_profiles(user_profiles) do |row|
      {
        user_id: user_id_from_imported_id(row[0]),
        website: (URI.parse(row[1]).to_s rescue nil),
        bio_raw: row[2],
      }
    end
  end

  def import_categories
    puts "Importing categories..."

    categories = mysql_query(<<-SQL
        SELECT node_id, parent_node_id, title, description, display_order
          FROM #{TABLE_PREFIX}node
         WHERE node_id > #{@last_imported_category_id}
         AND (node_type_id LIKE 'Category' OR node_type_id LIKE 'Forum')
      ORDER BY node_id
    SQL
    ).to_a

    return if categories.empty?

    parent_categories   = categories.select { |c| c[1] == 0 }
    children_categories = categories.select { |c| c[1] != 0 }

    parent_category_ids = Set.new parent_categories.map { |c| c[0] }

    # cut down the tree to only 2 levels of categories
    children_categories.each do |cc|
      until parent_category_ids.include?(cc[1])
        cc[1] = categories.find { |c| c[0] == cc[1] }[1]
      end
    end

    puts "Importing parent categories..."
    create_categories(parent_categories) do |row|
      {
        imported_id: row[0],
        name: normalize_text(row[2]),
        description: normalize_text(row[3]),
        position: row[4],
      }
    end

    puts "Importing children categories..."
    create_categories(children_categories) do |row|
      {
        imported_id: row[0],
        name: normalize_text(row[2]),
        description: normalize_text(row[3]),
        position: row[4],
        parent_category_id: category_id_from_imported_id(row[1]),
      }
    end
  end

  def import_topics
    puts "Importing topics..."

    topics = mysql_stream <<-SQL
        SELECT thread_id, title, node_id, user_id, discussion_open, post_date, view_count, discussion_state, sticky
          FROM #{TABLE_PREFIX}thread t
         WHERE thread_id > #{@last_imported_topic_id}
           AND EXISTS (SELECT 1 FROM #{TABLE_PREFIX}post p WHERE p.thread_id = t.thread_id)
      ORDER BY thread_id
    SQL

    create_topics(topics) do |row|
      created_at = Time.zone.at(row[5])

      topic_title = normalize_text(row[1])
      if topic_title.nil? || topic_title == ""
        topic_title = "You can't just have a topic with an empty title"
      end

      t = {
        imported_id: row[0],
        title: topic_title,
        category_id: category_id_from_imported_id(row[2]),
        user_id: user_id_from_imported_id(row[3]),
        closed: row[4] == 0,
        created_at: created_at,
        views: row[6],
        visible: row[7] == 'visible',
      }

      t[:pinned_at] = created_at if row[8] == 1

      t
    end
  end

  def import_posts
    puts "Importing posts..."

    posts = mysql_stream <<-SQL
        SELECT p.post_id, p.thread_id, p.user_id, p.post_date, p.message_state, p.message, p.reaction_score
          FROM #{TABLE_PREFIX}post p
         WHERE post_id > #{@last_imported_post_id}
      ORDER BY post_id
    SQL

    create_posts(posts) do |row|
      topic_id = topic_id_from_imported_id(row[1])

      post = {
        imported_id: row[0],
        topic_id: topic_id,
        user_id: user_id_from_imported_id(row[2]),
        created_at: Time.zone.at(row[3]),
        hidden: row[4] != 'visible',
        raw: normalize_text(row[5]),
        like_count: row[6],
      }
      post
    end
  end

  def import_likes
    puts "Importing likes..."

    @imported_likes = Set.new
    @last_imported_post_id = 0

    post_thanks = mysql_stream <<-SQL
        SELECT content_id, reaction_user_id, reaction_date, content_type
          FROM #{TABLE_PREFIX}reaction_content
         WHERE content_id > #{@last_imported_post_id}
         AND (content_type LIKE 'conversation_message' OR content_type LIKE 'post')
         GROUP BY content_id, reaction_user_id
      ORDER BY content_id
    SQL

    create_post_actions(post_thanks) do |row|
      post_id = post_id_from_imported_id(row[0])
      user_id = user_id_from_imported_id(row[1])

      next if post_id.nil? || user_id.nil?

      {
        post_id: post_id,
        user_id: user_id,
        post_action_type_id: 2,
        created_at: Time.zone.at(row[2])
      }
    end

    post_thanks = mysql_stream <<-SQL
        SELECT content_id, reaction_user_id, reaction_date, content_type, content_user_id
          FROM #{TABLE_PREFIX}reaction_content
         WHERE content_id > #{@last_imported_post_id}
         AND (content_type LIKE 'conversation_message' OR content_type LIKE 'post')
         GROUP BY content_id, reaction_user_id
      ORDER BY content_id
    SQL

    create_user_actions(post_thanks) do |row|
      post_id = post_id_from_imported_id(row[0])
      user_id = user_id_from_imported_id(row[1])
      actor = user_id_from_imported_id(row[4])
      if(row[3] == "post")
        topic_id = topic_id_from_imported_post_id(row[0])
      else
        topic_id = topic_id_from_imported_post_id(row[0] + PRIVATE_OFFSET)
      end

      next if post_id.nil? || user_id.nil? || actor.nil?

      {
        target_post_id: post_id,
        target_topic_id: topic_id,
        user_id: actor,
        target_user_id: user_id,
        action_type: 1,
        created_at: Time.zone.at(row[2]),
        updated_at: Time.zone.at(row[2])
      }
    end
  end

  def import_private_topics
    puts "Importing private topics..."

    @imported_topics = {}

    topics = mysql_stream <<-SQL
        SELECT m.conversation_id, m.title, m.user_id, m.recipients, m.start_date
          FROM #{TABLE_PREFIX}conversation_master m
         WHERE m.conversation_id > (#{@last_imported_private_topic_id - PRIVATE_OFFSET})
      ORDER BY m.conversation_id
    SQL

    create_topics(topics) do |row|
      if row[1]
        title = row[1]
      else
        title = "Message Title Missing!"
      end
      user_ids = [row[2], row[3].scan(/\"user_id\":(\d+)/)].flatten.map(&:to_i).sort
      key = [title, user_ids]

      next if @imported_topics.has_key?(key)
      @imported_topics[key] = row[0] + PRIVATE_OFFSET
      {
        archetype: Archetype.private_message,
        imported_id: row[0] + PRIVATE_OFFSET,
        title: title,
        user_id: user_id_from_imported_id(row[2]),
        created_at: Time.zone.at(row[4]),
      }
    end
  end

  def import_topic_allowed_users
    puts "Importing topic allowed users..."

    allowed_users = Set.new

    mysql_stream(<<-SQL
        SELECT m.conversation_id, m.recipients
          FROM #{TABLE_PREFIX}conversation_master m
         WHERE m.conversation_id > (#{@last_imported_private_topic_id - PRIVATE_OFFSET})
      ORDER BY m.conversation_id
    SQL
    ).each do |row|
      next unless topic_id = topic_id_from_imported_id(row[0] + PRIVATE_OFFSET)
      row[1].scan(/\"user_id\":(\d+)/).flatten.each do |id|
        next unless user_id = user_id_from_imported_id(id)
        allowed_users << [topic_id, user_id]
      end
    end

    create_topic_allowed_users(allowed_users) do |row|
      {
        topic_id: row[0],
        user_id: row[1],
      }
    end
  end

  def import_private_posts
    puts "Importing private posts..."

    posts = mysql_stream <<-SQL
        SELECT c.message_id, m.title, c.user_id, m.recipients, c.message_date, c.message, m.user_id
          FROM #{TABLE_PREFIX}conversation_message c
          INNER JOIN #{TABLE_PREFIX}conversation_master m ON c.conversation_id = m.conversation_id
         WHERE c.message_id > #{@last_imported_private_post_id - PRIVATE_OFFSET}
      ORDER BY c.message_id
    SQL

    create_posts(posts) do |row|
      if row[1]
        title = row[1]
      else
        title = "Message Title Missing!"
      end
      user_ids = [row[6], row[3].scan(/\"user_id\":(\d+)/)].flatten.map(&:to_i).sort
      key = [title, user_ids]

      next unless topic_id = topic_id_from_imported_id(@imported_topics[key])

      {
        imported_id: row[0] + PRIVATE_OFFSET,
        topic_id: topic_id,
        user_id: user_id_from_imported_id(row[2]),
        created_at: Time.zone.at(row[4]),
        raw: normalize_text(row[5]),
      }
    end
  end

  def import_topic_tags
    puts "Importing topic tags..."

    tag_mapping = {}
    prefix_mapping = {}

    mysql_query("SELECT t.tag_id AS ID, t.tag AS Name FROM #{TABLE_PREFIX}tag t").each do |row|
      tag_name = DiscourseTagging.clean_tag(row[1])
      tag = Tag.find_by_name(tag_name) || Tag.create(name: tag_name)
      tag_mapping[row[0]] = tag.id
    end

    mysql_query("SELECT CAST(TRIM(SUBSTRING_INDEX(phr.title, '.', -1)) AS UNSIGNED) AS prefix_id, phr.phrase_text FROM #{TABLE_PREFIX}phrase phr WHERE phr.title LIKE 'thread_prefix.%' ").each do |row|
      tag_name = DiscourseTagging.clean_tag(row[1])
      tag = Tag.find_by_name(tag_name) || Tag.create(name: tag_name)
      prefix_mapping[row[0]] = tag.id
    end

    topic_tags = mysql_stream <<-SQL
        SELECT tag_id, content_id FROM #{TABLE_PREFIX}tag_content
        WHERE content_type LIKE 'thread'
        AND tag_id IN(SELECT tag_id FROM #{TABLE_PREFIX}tag)
    SQL

    create_topic_tags(topic_tags) do |row|

      next unless topic_id = topic_id_from_imported_id(row[1])

      {
        tag_id: tag_mapping[row[0]],
        topic_id: topic_id
      }
    end

    topic_prefixes = mysql_stream <<-SQL
        SELECT t.prefix_id, t.thread_id FROM #{TABLE_PREFIX}thread t
        INNER JOIN #{TABLE_PREFIX}phrase phr ON(t.prefix_id = CAST(TRIM(SUBSTRING_INDEX(phr.title, '.', -1)) AS UNSIGNED))
        WHERE phr.title LIKE 'thread_prefix.%'
    SQL

    create_topic_tags(topic_prefixes) do |row|

      next unless topic_id = topic_id_from_imported_id(row[1])

      {
        tag_id: prefix_mapping[row[0]],
        topic_id: topic_id
      }
    end
  end

  def create_permalink_file
    puts '', 'Creating Permalink File...', ''

    id_mapping = []

    Topic.listable_topics.find_each do |topic|
      pcf = topic.first_post.custom_fields
      if pcf && pcf["import_id"]
        id = pcf["import_id"].split('-').last
        id_mapping.push("XXX#{id}  YYY#{topic.id}")
      end
    end

    # Category.find_each do |cat|
    #   ccf = cat.custom_fields
    #   if ccf && ccf["import_id"]
    #     id = ccf["import_id"].to_i
    #     id_mapping.push("/forumdisplay.php?#{id}  http://forum.quartertothree.com#{cat.url}")
    #   end
    # end

    CSV.open(File.expand_path("../vb_map.csv", __FILE__), "w") do |csv|
      id_mapping.each do |value|
        csv << [value]
      end
    end
  end

  # find the uploaded file information from the db
  def find_upload(post, attachment_id, mutex)

    mutex.synchronize do
      sql = "SELECT a.attachment_id, a.data_id, d.filename, d.file_hash, d.user_id
		    FROM #{TABLE_PREFIX}attachment AS a
		    INNER JOIN #{TABLE_PREFIX}attachment_data d ON a.data_id = d.data_id
		    WHERE attachment_id = #{attachment_id}"
      results = mysql_query(sql)
    end

    unless row = results.first
      puts "Couldn't find attachment record for attachment_id = #{attachment_id} post.id = #{post.id}"
      return
    end

    attachment_id = row[0]
    user_id = row[4]
    db_filename = row[2]
    data_id = row[1]
    file_hash = row[3]

    current_filename = "#{data_id}-#{file_hash}.data"

    filename = File.join(ATTACHMENT_DIR + "/#{data_id / 1000}/#{current_filename}")
    real_filename = db_filename
    real_filename.prepend SecureRandom.hex if real_filename[0] == '.'

    unless File.exists?(filename)
      puts "Attachment file #{row.inspect} doesn't exist"
      return nil
    end

    upload = create_upload(post.user.id, filename, real_filename)

    if upload.nil? || !upload.valid?
      puts "Upload not valid :("
      puts upload.errors.inspect if upload
      return
    end

    [upload, real_filename]
  rescue Mysql2::Error => e
    puts "SQL Error"
    puts e.message
    puts sql
  end

  def import_attachments
    puts '', 'importing attachments...'

    RateLimiter.disable
    current_count = 0
    success_count = 0
    fail_count = 0

    attachment_regex = /\[attach[^\]]*\](\d+)\[\/attach\]/i

    mutex = Mutex.new

    threads = []

    ATTACHMENT_IMPORTERS.times do |i|
      threads << Thread.new {
        total_count = 0
        attachment_stream = 0
        db_connect = PG.connect(dbname: db[:database], port: db[:port], user: "postgres")
        mutex.synchronize do
          result = db_connect.exec("SELECT COUNT(*) count FROM posts WHERE LOWER(raw) LIKE '%attach%' AND MOD(id, #{ATTACHMENT_IMPORTERS}) = #{i}")
          total_count = result[0]['count']
        end

        mutex.synchronize do
          db_connect.send_query("SELECT id FROM posts WHERE LOWER(raw) LIKE '%attach%' AND MOD(id, #{ATTACHMENT_IMPORTERS}) = #{i} ORDER BY id DESC")
          db_connect.set_single_row_mode
          attachment_stream = db_connect.get_result
        end

        attachment_stream.stream_each do |row|
          post = Post.find_by(id: row["id"])
          mutex.synchronize do
            current_count += 1
            print_status current_count, total_count
          end

          new_raw = post.raw.dup
          new_raw.gsub!(attachment_regex) do |s|
            matches = attachment_regex.match(s)
            attachment_id = matches[1]

            upload, filename = find_upload(post, attachment_id, mutex)
            unless upload
              mutex.synchronize do
                fail_count += 1
              end
              next
            # should we strip invalid attach tags?
            end

            next unless upload.sha1

            html_for_upload(upload, filename)
          end

          if new_raw != post.raw && post.topic
            PostRevisor.new(post).revise!(post.user, { raw: new_raw }, bypass_bump: true, edit_reason: 'Import attachments from xenForo', skip_validations: true, skip_revision: true)
          end

          mutex.synchronize do
            success_count += 1
          end
        end
      }
    end

    threads.each { |thr| thr.join }

    puts "", "imported #{success_count} attachments... failed: #{fail_count}."
    RateLimiter.enable
  end

  def import_avatars

    if AVATAR_DIR && File.exists?(AVATAR_DIR + '/h')
      puts "", "importing user avatars"

      User.find_each do |u|
        next unless u.custom_fields["import_id"]

        import_id = u.custom_fields["import_id"]
        dir = '0'
        if import_id.to_i >= 1000
          dir = (import_id.to_i / 1000).floor().to_s
        end

        photo_filename = AVATAR_DIR + '/h/' + dir + "/" + import_id + ".jpg"

        if !File.exists?(photo_filename)
          photo_filename = AVATAR_DIR + '/o/' + dir + "/" + import_id + ".jpg"

          if !File.exists?(photo_filename)
            puts "Path to avatar file not found! Skipping. #{photo_filename}"
            next
          end
        end

        print "."

        upload = create_upload(u.id, photo_filename, File.basename(photo_filename))
        if upload.persisted?
          u.import_mode = false
          u.create_user_avatar
          u.import_mode = true
          u.user_avatar.update(custom_upload_id: upload.id)
          u.update(uploaded_avatar_id: upload.id)
        else
          puts "Error: Upload did not persist for #{u.username} #{photo_real_filename}!"
        end
      end
    end
  end

  USER_ACTION_COLUMNS ||= %i{
    action_type user_id target_topic_id target_post_id target_user_id
    acting_user_id created_at updated_at
  }

  def create_user_actions(rows, &block)
    create_records(rows, "user_action", USER_ACTION_COLUMNS, &block)
  end

  def create_records(rows, name, columns)
    start = Time.now
    imported_ids = []
    process_method_name = "process_#{name}"
    sql = "COPY #{name.pluralize} (#{columns.map { |c| "\"#{c}\"" }.join(",")}) FROM STDIN"

    @raw_connection.copy_data(sql, @encoder) do
      rows.each do |row|
        begin
          next unless mapped = yield(row)
          processed = send(process_method_name, mapped)
          imported_ids << mapped[:imported_id] unless mapped[:imported_id].nil?
          imported_ids |= mapped[:imported_ids] unless mapped[:imported_ids].nil?
          @raw_connection.put_copy_data columns.map { |c| processed[c] }
          print "\r%7d - %6d/sec" % [imported_ids.size, imported_ids.size.to_f / (Time.now - start)] if imported_ids.size % 5000 == 0
        rescue => e
          puts "\n"
          puts "ERROR: #{e.message}"
          puts e.backtrace.join("\n")
        end
      end
    end

    if imported_ids.size > 0
      print "\r%7d - %6d/sec" % [imported_ids.size, imported_ids.size.to_f / (Time.now - start)]
      puts
    end

    id_mapping_method_name = "#{name}_id_from_imported_id".freeze
    return unless respond_to?(id_mapping_method_name)
    create_custom_fields(name, "id", imported_ids) do |imported_id|
      {
        record_id: send(id_mapping_method_name, imported_id),
        value: imported_id,
      }
    end
  rescue => e
    puts e.message
    puts e.backtrace.join("\n")
  end

  def process_user_action(user_action)
    user_action[:target_topic_id] ||= nil
    user_action[:target_post_id] ||= nil
    user_action[:target_user_id] ||= nil
    user_action[:created_at] ||= NOW
    user_action[:updated_at] ||= NOW
    user_action
  end

  def process_post(post)
    @posts[post[:imported_id].to_i] = post[:id] = @last_post_id += 1
    post[:user_id] ||= Discourse::SYSTEM_USER_ID
    post[:last_editor_id] = post[:user_id]
    @highest_post_number_by_topic_id[post[:topic_id]] ||= 0
    post[:post_number] = @highest_post_number_by_topic_id[post[:topic_id]] += 1
    post[:sort_order] = post[:post_number]
    @post_number_by_post_id[post[:id]] = post[:post_number]
    @topic_id_by_post_id[post[:id]] = post[:topic_id]
    post[:raw] = (post[:raw] || "").scrub.strip.presence || "<Empty imported post>"
    post[:raw] = process_raw post[:raw]
    if @bbcode_to_md
      post[:raw] = post[:raw].bbcode_to_md(false, {}, :disable, :quote) rescue post[:raw]
    end
    post[:like_count] ||= 0
    post[:cooked] = pre_cook post[:raw]
    post[:hidden] ||= false
    post[:word_count] = post[:raw].scan(/[[:word:]]+/).size
    post[:created_at] ||= NOW
    post[:last_version_at] = post[:created_at]
    post[:updated_at] ||= post[:created_at]
    post
  end

  def process_raw(original_raw)
    raw = original_raw.dup
    # fix whitespaces
    raw.gsub!(/(\\r)?\\n/, "\n")
    raw.gsub!("\\t", "\t")

    # [CODE]...[/CODE]
    raw.gsub!(/\[\/?CODE\]/i, "\n\n```\n\n")

    # replace all chevrons with HTML entities
    # /!\ must be done /!\
    #  - AFTER the "code" processing
    #  - BEFORE the "quote" processing
    raw.gsub!(/`([^`]+?)`/im) { "`" + $1.gsub("<", "\u2603") + "`" }
    raw.gsub!("<", "&lt;")
    raw.gsub!("\u2603", "<")

    raw.gsub!(/`([^`]+?)`/im) { "`" + $1.gsub(">", "\u2603") + "`" }
    raw.gsub!(">", "&gt;")
    raw.gsub!("\u2603", ">")

    raw.gsub!(/\[\/?I\]/i, "*")
    raw.gsub!(/\[\/?B\]/i, "**")
    raw.gsub!(/\[\/?U\]/i, "")

    # [IMG]...[/IMG]
    raw.gsub!(/(?:\s*\[IMG\]\s*)+(.+?)(?:\s*\[\/IMG\]\s*)+/im) { "\n\n#{$1}\n\n" }

    # [IMG=url]
    raw.gsub!(/\[IMG=([^\]]*)\]/im) { "\n\n#{$1}\n\n" }

    # [URL=...]...[/URL]
    raw.gsub!(/\[URL="?(.+?)"?\](.+?)\[\/URL\]/im) { "[#{$2.strip}](#{$1})" }

    # [URL]...[/URL]
    # [LEFT]...[/LEFT]
    raw.gsub!(/\[\/?URL\]/i, "")
    #raw.gsub!(/\[\/?LEFT\]/i, "")

    raw.gsub!(/\[SIZE=.*?\](.*?)\[\/SIZE\]/im, "\\1")
    raw.gsub!(/\[H=.*?\](.*?)\[\/H\]/im, "\\1")

    # [INDENT]...[/INDENT]
    raw.gsub!(/\[INDENT\](.*?)\[\/INDENT\]/im, "\\1")
    raw.gsub!(/\[TABLE\](.*?)\[\/TABLE\]/im, "\\1")
    raw.gsub!(/\[TR\](.*?)\[\/TR\]/im, "\\1")
    raw.gsub!(/\[TD\](.*?)\[\/TD\]/im, "\\1")
    raw.gsub!(/\[TD="?.*?"?\](.*?)\[\/TD\]/im, "\\1")

    # Nested Quotes
    raw.gsub!(/(\[\/?QUOTE.*?\])/mi) { |q| "\n#{q}\n" }

    # [QUOTE=<username>, <postid>, <userid>]
    raw.gsub!(/\[quote="([\w\s]+), post: (\d*), member: (\d*)"\]/i) do
      imported_username, imported_postid, imported_userid = $1.gsub(/\s+/, ""), $2, $3

      username = @mapped_usernames[imported_username] || imported_username
      post_number = post_number_from_imported_id(imported_postid)
      topic_id = topic_id_from_imported_post_id(imported_postid)

      if post_number && topic_id
        "\n[quote=\"#{username}, post:#{post_number}, topic:#{topic_id}\"]\n"
      else
        "\n[quote=\"#{username}\"]\n"
      end
    end

    # [SPOILER=Some hidden stuff]SPOILER HERE!![/SPOILER]
    raw.gsub!(/\[SPOILER="?(.+?)"?\](.+?)\[\/SPOILER\]/im) { "\n#{$1}\n[spoiler]#{$2}[/spoiler]\n" }

    # convert list tags to ul and list=1 tags to ol
    # (basically, we're only missing list=a here...)
    # (https://meta.discourse.org/t/phpbb-3-importer-old/17397)
    raw.gsub!(/\[list\](.*?)\[\/list\]/im, '[ul]\1[/ul]')
    raw.gsub!(/\[list=1\](.*?)\[\/list\]/im, '[ol]\1[/ol]')
    raw.gsub!(/\[list\](.*?)\[\/list:u\]/im, '[ul]\1[/ul]')
    raw.gsub!(/\[list=1\](.*?)\[\/list:o\]/im, '[ol]\1[/ol]')
    # convert *-tags to li-tags so bbcode-to-md can do its magic on phpBB's lists:
    raw.gsub!(/\[\*\]\n/, '')
    raw.gsub!(/\[\*\](.*?)\[\/\*:m\]/, '[li]\1[/li]')
    raw.gsub!(/\[\*\](.*?)\n/, '[li]\1[/li]')
    raw.gsub!(/\[\*=1\]/, '')

    if ! raw.valid_encoding?
      raw = raw.encode("UTF-16be", invalid: :replace, replace: "?").encode('UTF-8')
    end

    raw.gsub!(/\x00/, '')

    raw
  end

  def extract_pm_title(title)
    normalize_text(title).scrub.gsub(/^Re\s*:\s*/i, "")
  end

  def parse_birthday(birthday)
    return if birthday.blank?
    date_of_birth = Date.strptime(birthday.gsub(/[^\d-]+/, ""), "%m-%d-%Y") rescue nil
    return if date_of_birth.nil?
    date_of_birth.year < 1904 ? Date.new(1904, date_of_birth.month, date_of_birth.day) : date_of_birth
  end

  def print_status(current, max, start_time = nil)
    if start_time.present?
      elapsed_seconds = Time.now - start_time
      elements_per_minute = '[%.0f items/min]  ' % [current / elapsed_seconds.to_f * 60]
    else
      elements_per_minute = ''
    end

    print "\r%9d / %d (%5.1f%%)  %s" % [current, max, current / max.to_f * 100, elements_per_minute]
  end

  def mysql_stream(sql)
    @client.query(sql, stream: true)
  end

  def mysql_query(sql)
    @client.query(sql)
  end

end

BulkImport::XenForo.new.run
