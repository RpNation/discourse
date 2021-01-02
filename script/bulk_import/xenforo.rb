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

    #import_groups
    import_users
    #import_group_users

    import_user_emails
    import_user_stats

    import_user_profiles

    import_categories
    import_topics
    import_posts

    import_private_topics
    import_topic_allowed_users
    import_private_posts

    import_likes

    #create_permalink_file
    #import_attachments
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
        username: normalize_text(row[1]),
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

      t = {
        imported_id: row[0],
        title: normalize_text(row[1]),
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
      title = extract_pm_title(row[1])
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
      title = extract_pm_title(row[1])
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
  def find_upload(post, attachment_id)
    sql = "SELECT a.attachmentid attachment_id, a.userid user_id, a.filename filename,
                  a.filedata filedata, a.extension extension
             FROM #{TABLE_PREFIX}attachment a
            WHERE a.attachmentid = #{attachment_id}"
    results = mysql_query(sql)

    unless row = results.first
      puts "Couldn't find attachment record for attachment_id = #{attachment_id} post.id = #{post.id}"
      return
    end

    attachment_id = row[0]
    user_id = row[1]
    db_filename = row[2]

    filename = File.join(ATTACHMENT_DIR, user_id.to_s.split('').join('/'), "#{attachment_id}.attach")
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

    total_count = mysql_query(<<-SQL
      SELECT COUNT(p.postid) count
        FROM #{TABLE_PREFIX}post p
        JOIN #{TABLE_PREFIX}thread t ON t.threadid = p.threadid
       WHERE t.firstpostid <> p.postid
    SQL
    ).first[0].to_i

    success_count = 0
    fail_count = 0

    attachment_regex = /\[attach[^\]]*\](\d+)\[\/attach\]/i

    Post.find_each do |post|
      current_count += 1
      print_status current_count, total_count

      new_raw = post.raw.dup
      new_raw.gsub!(attachment_regex) do |s|
        matches = attachment_regex.match(s)
        attachment_id = matches[1]

        upload, filename = find_upload(post, attachment_id)
        unless upload
          fail_count += 1
          next
          # should we strip invalid attach tags?
        end

        html_for_upload(upload, filename)
      end

      if new_raw != post.raw
        PostRevisor.new(post).revise!(post.user, { raw: new_raw }, bypass_bump: true, edit_reason: 'Import attachments from vBulletin')
      end

      success_count += 1
    end

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
