"""Two-year daily leadership reading and devotional plan.

Year 1 (2026, 2028, ...): personal character and faithful service.
Year 2 (2027, 2029, ...): developing people and a healthy ministry.
Each year has 52 seven-day studies and a year-end reflection (365 entries).
February 29 has its own entry and never displaces the March readings. The
calendar repeats after two years, with 730 ordinary entries and one leap entry.

The reflections below are original companion material, not John Maxwell's
writing. A matching article from the user's indexed Maxwell Leadership Bible
can accompany each day's reflection; the book itself is not bundled here.
No model, external service or indexed book is needed for daily availability.
"""

from datetime import date


PLAN_VERSION = "leadership-two-year-v1"
ORIGINAL_SOURCE = "Original leadership devotional · Maxwell Leadership Bible companion plan"
ANCHOR_YEAR = 2026

# Each row defines a seven-verse reading, its theme, and distinct reflections
# for the two years. Read the whole chapter alongside the selected daily verse.
# Keep the sequence here, rather than in app.py, to make editorial changes easy.
_WEEKS = (
    ("Genesis", 1, 24, "Stewardship begins with God",
     "Creation belongs to God before anyone receives responsibility for it. Your abilities and opportunities are entrusted gifts. Treat the people and resources in your care with the patience of a steward rather than the entitlement of an owner.",
     "Help your team see people as God's image bearers. A person's usefulness never measures their worth. Ask how your decisions protect dignity, invite participation and care for what God has entrusted to the whole community."),
    ("Genesis", 12, 1, "Responding to God's call",
     "Abram's journey begins with God's invitation, not a complete map. Faithful leadership often starts with one obedient step. Name what God has already made clear before demanding certainty about everything ahead.",
     "A calling becomes a blessing when it reaches beyond the leader. Help someone connect their gifts to the needs of others. Make room for a faithful first step without requiring them to know every future detail."),
    ("Genesis", 39, 1, "Integrity under pressure",
     "Joseph's circumstances change while his responsibility to God remains. Integrity matters when you are overlooked as much as when you are trusted. Choose honesty in the task before you, even when recognition is uncertain.",
     "Healthy teams need integrity that does not depend on supervision. Be clear about boundaries, give appropriate trust and refuse to reward shortcuts. Protect people who do the right thing at a personal cost."),
    ("Genesis", 41, 33, "Preparing with wisdom",
     "Joseph's counsel joins discernment to practical preparation. Spiritual concern should lead to responsible planning. Review the resources available now and make a wise provision for needs that may arrive later.",
     "Preparation serves the community when responsibility is shared. Invite others to help identify risks and organise resources. Explain why the plan matters so that people can contribute understanding as well as effort."),
    ("Exodus", 3, 7, "Listening before leading",
     "God notices suffering before Moses receives his assignment. Leadership begins with attention to people rather than ambition for a role. Listen long enough to understand a burden before offering your solution.",
     "Teach emerging leaders to notice people whose pain rarely reaches a meeting agenda. Invite those affected by a problem to speak. Let their experience shape both the response and the way you measure whether help has reached them."),
    ("Exodus", 4, 10, "Serving despite limitations",
     "Moses brings his limitations into a conversation with God. Honest dependence is different from pretending to be able to do everything. Admit where you need help and take the next responsibility with humility.",
     "Another person's limitation is an invitation to wise support, not a reason to dismiss their calling. Find the training, partnership or encouragement that makes faithful service possible. Share responsibility without shaming their weakness."),
    ("Exodus", 18, 17, "Sharing responsibility",
     "Moses needs counsel about a burden he cannot carry alone. Exhaustion can hide behind a sincere desire to help everyone. Accept correction and distinguish the work you must do from the work others can learn to carry.",
     "Delegation involves choosing trustworthy people, clarifying responsibility and remaining available. Give someone a meaningful task with guidance and authority. Check that your support develops their judgment rather than making them depend on you for every choice."),
    ("Exodus", 20, 1, "Faithfulness before success",
     "God's commands place loyalty to him before human achievement. A successful project cannot repair a divided heart. Examine which demand receives your first attention and allow worship to reorder your priorities.",
     "A ministry's pace can quietly teach people what it worships. Discuss how schedules, praise and spending reflect your stated values. Lead the team into practices that honour God even when those practices produce less visible activity."),
    ("Numbers", 13, 25, "Seeing difficulty with faith",
     "The spies encounter the same land but interpret its challenges differently. Faith does not require hiding facts. Bring both the difficulty and God's faithfulness into view before fear decides what you will do.",
     "When a team is afraid, listen carefully and make space for honest questions. Help people separate verified obstacles from assumptions. Encourage a response that is prayerful and practical rather than dismissing their concerns with slogans."),
    ("Deuteronomy", 6, 4, "Teaching through everyday life",
     "Love for God takes root in ordinary conversations and repeated practices. Your daily habits teach before your formal words do. Choose one ordinary moment in which to give God your attention today.",
     "Developing people requires more than an occasional lesson. Model faithfulness in shared work, meals and conversations. Give a younger believer space to ask questions and show how Scripture shapes an actual decision."),
    ("Joshua", 1, 3, "Courage with obedience",
     "Joshua faces a task larger than his experience. The call to courage is joined to attention to God's instruction. Let courage grow through obedience to what is right rather than confidence that you cannot fail.",
     "People stepping into responsibility need encouragement and clear foundations. Walk beside someone facing a new assignment. Help them identify the instructions, relationships and habits that will keep their courage accountable to God."),
    ("Joshua", 24, 14, "Choosing whom to serve",
     "Joshua asks the people to make their allegiance clear. Faithfulness cannot rest only on yesterday's commitment. Consider where convenience is competing with devotion and make a deliberate choice to serve God today.",
     "A public commitment needs support in shared life. Invite your team to describe what serving God will mean in a particular decision. Agree on a concrete practice and revisit it with patience instead of settling for a memorable statement."),
    ("Judges", 6, 11, "Answering a difficult assignment",
     "Gideon's questions emerge in a season of fear and hardship. Bring your uncertainty honestly before God. You do not need to manufacture confidence before offering the strength and willingness you have.",
     "Potential is easy to miss in people who are afraid. Encourage someone without exaggerating their readiness or ignoring their questions. Offer a manageable responsibility and remain present as they learn to respond faithfully."),
    ("Ruth", 2, 8, "Creating a place of kindness",
     "Boaz's treatment of Ruth connects kindness with practical protection. Good intentions matter most when they change another person's experience. Notice who may feel exposed or unwelcome and make your care specific.",
     "Hospitality becomes credible through a group's daily conduct. Ask newcomers whether they understand how to participate and whom to approach for help. Build safeguards that do not depend on a single leader happening to notice a need."),
    ("1 Samuel", 3, 1, "Learning to listen",
     "Samuel learns to recognise a voice he does not yet understand. Listening requires humility and often the help of someone wiser. Slow your response and test your understanding before turning an impression into a decision.",
     "Eli helps Samuel respond without claiming the conversation for himself. Mentor people toward attentiveness to God rather than dependence on your approval. Ask questions that strengthen discernment and give them room to respond."),
    ("1 Samuel", 16, 6, "Looking beyond appearances",
     "Samuel needs correction about the way he recognises a leader. Appearance and confidence can draw attention away from character. Examine the qualities you admire and ask whether your choices honour the heart God sees.",
     "Selection decisions shape a ministry's future. Look for faithfulness and teachability as carefully as visible ability. Give overlooked people opportunities to demonstrate character through service rather than expecting everyone to impress in the same way."),
    ("1 Samuel", 17, 32, "Faithful preparation",
     "David brings the experience of ordinary responsibility into an extraordinary challenge. Hidden faithfulness is not wasted time. Consider what your present task is teaching you and resist the urge to despise a small assignment.",
     "Help developing leaders connect past faithfulness to present responsibility. Avoid forcing them into another person's methods. Offer preparation, realistic feedback and enough freedom to serve with the gifts they actually have."),
    ("1 Samuel", 24, 4, "Restraint with power",
     "David's opportunity to harm Saul is not the same as permission to do it. Power tests whether conviction survives advantage. Refuse a shortcut that violates your conscience even when others can supply a convincing justification.",
     "Your response to conflict teaches the team what power is for. Establish limits on retaliation, gossip and public humiliation. Protect the dignity of difficult people while addressing wrongdoing through fair and accountable action."),
    ("2 Samuel", 9, 1, "Remembering commitments",
     "David looks for a way to show kindness because of a promise. Relationships should not disappear when they stop being useful. Remember someone to whom you owe care and take a practical step toward keeping your word.",
     "Build a team that remembers commitments to people who cannot advance its reputation. Review promises made to volunteers, families and partners. Assign responsibility for following through so that kindness survives changes in personnel."),
    ("1 Kings", 3, 5, "Asking for wisdom",
     "Solomon recognises that responsibility exceeds his understanding. Wisdom begins when you stop pretending otherwise. Before making a decision that affects others, ask God for discernment and seek the information you still lack.",
     "Teach others how to ask wise questions instead of simply borrowing your answer. Discuss who will be affected, what is uncertain and which biblical responsibilities apply. Let prayer and careful listening shape a shared decision."),
    ("1 Kings", 19, 3, "Receiving rest and care",
     "Elijah's exhaustion is met with care before another assignment. Physical weakness and discouragement deserve attention. Receive rest, food and companionship without treating your limits as evidence that you have failed God.",
     "A healthy ministry makes room for recovery. Notice when reliable people have become depleted and offer practical relief. Adjust workloads and expectations so that care is an ordinary part of leadership rather than an emergency gesture."),
    ("2 Kings", 5, 9, "Humility to receive help",
     "Naaman nearly rejects help because it arrives in an unexpected form. Pride can make a simple act of trust feel beneath you. Listen for useful counsel from someone whose status does not impress you.",
     "A team's wisdom may come through its least prominent member. Make it safe to offer correction across roles. Thank people for useful observations and show, through a changed decision, that listening was more than a courtesy."),
    ("Nehemiah", 1, 4, "Turning concern into prayer",
     "Nehemiah carries the community's trouble into prayer. He does not rush from emotion into activity. Bring a specific burden to God and acknowledge your own responsibility before deciding what action to take.",
     "Shared prayer can prepare a team for honest ownership. Give people a clear picture of the need and time to respond before assigning work. Let confession, gratitude and dependence accompany planning instead of serving as a brief introduction."),
    ("Nehemiah", 2, 11, "Understanding before acting",
     "Nehemiah inspects the damage before calling people to rebuild. Careful observation helps concern become a responsible plan. Examine the situation quietly and verify what needs attention before making public promises.",
     "Invite people into a task with truthful information and a clear purpose. Explain the need, the work and the support available. Leave room for their knowledge to improve the plan rather than asking only for agreement."),
    ("Nehemiah", 4, 13, "Protecting shared work",
     "Rebuilding requires attention to danger as well as progress. Trust in God does not excuse neglect of practical safeguards. Identify a real vulnerability and take a proportionate step to protect the people involved.",
     "Teams persevere when people know how to support one another. Clarify how concerns will be raised, who responds and how help is requested. Pair encouragement with sensible arrangements that reduce avoidable strain."),
    ("Esther", 4, 10, "Courage for others",
     "Esther must consider responsibility beyond her own safety. Courage grows when another person's need becomes harder to ignore than your discomfort. Pray carefully about an appropriate action on behalf of someone with less influence.",
     "When a colleague faces a costly decision, offer solidarity rather than pressure from a safe distance. Discuss the risks honestly and share whatever burden you can. Honour their agency while helping them act with conviction."),
    ("Psalms", 23, 1, "Leading as a shepherd",
     "The shepherd's care includes provision, guidance and presence in danger. Receive God's care before trying to provide everything for others. Remember that faithful leadership can be quiet, patient and close at hand.",
     "Consider whether the people you lead experience guidance as care. Explain expectations, notice weariness and stay present in difficulty. Help another leader learn the difference between directing a task and tending the people carrying it."),
    ("Psalms", 78, 1, "Passing faith to another generation",
     "The psalm calls God's people to remember and tell what he has done. Your experience becomes a gift when you share it honestly. Tell one story of God's faithfulness without editing out your dependence or mistakes.",
     "A community's memory needs more than its oldest leader. Invite different generations to listen to one another and record lessons worth keeping. Give younger people opportunities to retell the story and ask their own questions."),
    ("Psalms", 139, 1, "Living with an examined heart",
     "God knows what lies beyond public appearance. Honest prayer makes room for motives you would rather avoid. Bring one hidden fear or desire into his presence and ask how it is influencing your treatment of others.",
     "Self-awareness helps leaders receive correction without defensiveness. Model a specific admission of your own limitations. Invite trusted feedback and show how accountability can lead to repair rather than loss of dignity."),
    ("Proverbs", 3, 1, "Trust expressed in practice",
     "Trust in God touches decisions, relationships and resources. It is more than a reassuring feeling. Name one area in which self-reliance is directing your choices and practise bringing it under God's instruction.",
     "Help your team connect trust to a concrete decision. Consider how your use of time, money and attention reveals what you rely on. Make an adjustment together and return later to discuss what it taught you."),
    ("Proverbs", 4, 20, "Guarding the inner life",
     "Attention shapes the heart, and the heart shapes the direction of life. What you repeatedly welcome will influence the way you lead. Choose one habit of listening, looking or speaking that needs a healthier boundary.",
     "A team's atmosphere is formed through repeated habits. Notice what your meetings reward and what your conversations normalise. Help the group replace a harmful pattern with a practice that supports truth, patience and faithfulness."),
    ("Proverbs", 15, 1, "Words that build trust",
     "Speech can lower the temperature of a disagreement or make it burn hotter. A gentle answer still needs to be truthful. Pause before your next difficult reply and choose words that make understanding possible.",
     "Teach people to handle disagreement without contempt. Practise stating a concern clearly, listening without interruption and checking what was heard. Praise the courage to repair a conversation as much as the ability to make a persuasive argument."),
    ("Ecclesiastes", 4, 6, "Choosing partnership over isolation",
     "Relentless effort can leave a person without companionship or contentment. Consider whether your pace is separating you from the people who give life its meaning. Accept help and make time for a relationship that needs attention.",
     "Partnership requires arrangements that allow people to help and be helped. Look for isolated workers in your ministry. Build opportunities for companionship, shared learning and honest support instead of merely adding another meeting."),
    ("Isaiah", 6, 1, "Reverence before responsibility",
     "Isaiah's encounter with God brings awe and an honest awareness of need. Service begins in grace rather than self-importance. Approach today's work as a response to God's mercy, with room for repentance and gratitude.",
     "Keep worship from becoming a tool for recruiting workers. Give people space to encounter God, confess and receive grace. Their response to responsibility should grow from devotion rather than pressure to prove their usefulness."),
    ("Isaiah", 40, 25, "Strength through dependence",
     "The prophet sets human frailty beside God's greatness. Weariness is a reason to turn toward him rather than manufacture an image of endless strength. Name your limits and receive the help appropriate to this day.",
     "Leaders can unintentionally teach others to hide exhaustion. Make honest conversations about capacity welcome. Plan work at a sustainable pace and remind the team that God's faithfulness does not depend on anyone being indispensable."),
    ("Jeremiah", 1, 4, "Growing into responsibility",
     "Jeremiah's hesitation does not end the conversation about his calling. Inexperience invites learning and dependence. Take your questions seriously while remaining willing to obey the responsibility already placed before you.",
     "Encourage developing leaders without demanding instant maturity. Provide preparation, feedback and a trusted person to consult. Give them responsibility in stages so that courage and competence can grow together."),
    ("Micah", 6, 2, "Justice and humble faithfulness",
     "Micah brings God's people back to the substance of faithfulness. Religious activity cannot replace justice, mercy and humility. Examine one ordinary relationship in which these qualities need to become visible through action.",
     "Review a policy or habit that affects vulnerable people. Ask whose voice is missing and whether the process is fair. Lead a concrete improvement that expresses mercy without abandoning responsibility or truth."),
    ("Matthew", 5, 3, "The character of God's kingdom",
     "Jesus describes a blessed life in terms that challenge ordinary measures of success. Humility, mercy and peacemaking deserve your attention. Choose one of these qualities to practise where ambition has made you impatient.",
     "A team's definition of success forms its people. Notice whether humility and mercy are recognised alongside results. Encourage a person whose quiet faithfulness strengthens the community even when it produces little public attention."),
    ("Matthew", 6, 5, "Prayer without performance",
     "Jesus draws attention away from impressive appearances and toward a sincere relationship with the Father. Bring your actual needs into prayer. Let the hidden life with God support the work that other people can see.",
     "Avoid measuring spiritual maturity by public fluency. Make room for simple, honest prayer and listen to quieter members. Help the group practise dependence on God in ways that do not turn devotion into a competition."),
    ("Matthew", 7, 24, "Putting truth into practice",
     "Hearing Jesus' words needs to become obedience. A strong foundation is laid through decisions repeated over time. Identify a teaching you already understand and take one practical step that lets it shape your behaviour.",
     "Training is incomplete until people can practise what they learn. Follow a lesson with a manageable assignment, support and reflection. Ask what changed in daily conduct rather than counting attendance as proof of growth."),
    ("Matthew", 20, 20, "Greatness through service",
     "Jesus meets the desire for status with a different account of greatness. Consider whom you are willing to serve when the task brings no recognition. Let care for a person matter more than protecting your importance.",
     "Responsibility should enlarge a leader's willingness to serve. Review whether roles distribute honour while leaving unpleasant work to the same people. Share burdens and give visible respect to those whose service is usually unnoticed."),
    ("Mark", 3, 13, "Growing through shared life",
     "Jesus calls people into companionship as well as responsibility. Do not reduce discipleship to completing tasks. Make space to be with God and with people who help you become more faithful.",
     "Developing leaders involves time together, observation and practice. Invite someone into an appropriate part of your work and explain the reasons behind your decisions. Leave space for questions and for learning through relationship."),
    ("Mark", 10, 42, "Authority that serves",
     "Jesus challenges the use of power to dominate others. Authority carries a responsibility to seek their good. Notice one place where impatience has become control and choose a more patient, respectful response.",
     "Give people clear responsibilities without treating them as extensions of yourself. Invite honest disagreement and share credit for what goes well. Use your position to remove obstacles that prevent others from serving well."),
    ("Luke", 10, 30, "Compassion that crosses boundaries",
     "The Samaritan's compassion becomes costly, practical care. A neighbour's need can interrupt your preferred schedule. Notice whom you have learned to pass by and offer an appropriate act of help.",
     "A compassionate team looks beyond familiar circles. Ask who benefits from your ministry and who remains outside its attention. Partner wisely with people who understand the need and make care sustainable rather than merely dramatic."),
    ("Luke", 15, 11, "Grace and restoration",
     "Jesus' story exposes both wandering and the need for a welcome home. Reflect on the grace you need to receive and extend. Make room for repentance without assuming you know the whole of another person's story.",
     "Restoration calls for compassion, truth and wise boundaries. Help the community distinguish welcoming a person from rushing them back into responsibility. Create patient steps toward trust while protecting those affected by earlier harm."),
    ("John", 13, 12, "Leading by example",
     "After washing his disciples' feet, Jesus asks them to consider his example. What you practise gives weight to what you teach. Choose a task today that expresses the service you hope to see in others.",
     "An example becomes more useful when people understand its purpose. Invite a developing leader to serve beside you and discuss what the task reveals about responsibility. Encourage them to make the same care visible in their own relationships."),
    ("John", 15, 1, "Fruitfulness through abiding",
     "Jesus places abiding before fruitfulness. Your life with him cannot be sustained by activity alone. Protect time for his words and prayer, and release the need to prove your worth through constant output.",
     "Help people develop practices that nourish faith beyond a ministry assignment. Ask whether the team's pace leaves space for prayer, Scripture and relationships. Celebrate lasting character and love alongside completed projects."),
    ("Acts", 6, 1, "Responding to neglected needs",
     "The early church listens when a group reports being overlooked. Growth does not excuse an unfair pattern. Receive a concern without defensiveness and consider what responsibility you have to help address it.",
     "Address a recurring need through a fair process and suitable people. Explain how responsibilities will be shared and how concerns can be heard. Let an effective structure support care rather than replacing personal attention."),
    ("Romans", 12, 3, "Different gifts, shared purpose",
     "Paul joins sober self-assessment to the recognition of many gifts. You do not need to do everything to belong. Offer your ability faithfully and receive another person's contribution without comparison.",
     "Match responsibility to gifts while leaving room for growth. Invite people to describe where they serve well and where support is needed. Build cooperation so that the group's strength comes from mutual service rather than competition."),
    ("Ephesians", 4, 11, "Equipping others to serve",
     "Ministry helps people grow toward maturity in Christ. Consider where you are still expecting someone else to do the work you could learn to carry. Ask for guidance and accept a suitable next responsibility.",
     "Equipping requires teaching, practice and encouragement. Choose one task you routinely keep and help someone learn it. Aim for their growth and the health of the body rather than simply reducing your own workload."),
    ("Philippians", 2, 1, "Humility in community",
     "Paul connects unity to the willingness to attend to others' interests. Humility makes space for another person's experience. In a shared decision today, ask what the other person needs before defending your preference.",
     "Unity grows through habits that resist rivalry. Share credit, invite quieter voices and acknowledge when another idea is better. Address division honestly while modelling the concern for others that you ask the team to practise."),
    ("2 Timothy", 2, 1, "Passing responsibility faithfully",
     "Paul looks beyond a single relationship to faithful people who can teach others. Receive what has been entrusted to you with gratitude. Identify one lesson worth practising carefully and passing on truthfully.",
     "A ministry's future depends on people who can serve after you. Invest in a trustworthy person, document what they need and give them real practice. Success includes their ability to help someone else grow without your constant presence."),
)

# Psalms 23 has only six verses; its seventh day revisits the whole psalm's
# opening confession. Other studies use seven consecutive verses.
_DAILY_FOCUS = (
    ("Notice", "Read the verse in its chapter. What does the passage actually say before you apply it?", "Write one observation in your own words and name something you still need to understand.", "Lord, help me listen carefully and receive your truth."),
    ("Examine", "Where does this reading expose a motive, habit or assumption in your own leadership?", "Name one specific attitude to bring honestly before God today.", "Lord, search my heart and make me willing to change."),
    ("Listen", "How might someone affected by your decisions experience the concern raised in this passage?", "Ask a trusted person a sincere question and listen without preparing a defence.", "Lord, give me humility to hear another person's experience."),
    ("Practise", "What small act of obedience would make this lesson visible in ordinary life?", "Choose one achievable action, decide when to do it and follow through.", "Lord, turn what I understand into faithful action."),
    ("Encourage", "Who could be strengthened by patient care connected to this week's reading?", "Offer one specific word of encouragement together with practical help where appropriate.", "Lord, help my words and actions strengthen someone today."),
    ("Share", "How could you pass on this lesson without making yourself the centre of the story?", "Discuss the passage with one person and invite their observations before offering your own.", "Lord, make me a faithful learner and a generous companion."),
    ("Review", "What has changed through this week's reading, and what still needs attention?", "Record one lesson, give thanks for one gift and choose a next step for the coming week.", "Lord, deepen what you have begun and help me remain faithful."),
)


def _entry(year, index):
    week, weekday = divmod(index, 7)
    book, chapter, first, theme, personal, team = _WEEKS[week]
    verse = first + weekday
    if book == "Psalms" and chapter == 23 and weekday == 6:
        verse = 1
    # Matthew 7 ends at verse 29: day seven returns to the opening verse.
    if book == "Matthew" and chapter == 7 and weekday == 6:
        verse = first
    focus, question, action, prayer = _DAILY_FOCUS[weekday]
    reference = f"{book} {chapter}:{verse}"
    reflection = personal if year == 1 else team
    development = ("Consider your own example and the habits you are forming." if year == 1 else
                   "Consider how you can help another person learn this through shared responsibility.")
    content = (f"{reflection}\n\nToday's reading: {reference}. Read the surrounding chapter for context. "
               f"{development}\n\nReflect · {focus}\n{question}\n\nPractise\n{action}\n\nPrayer\n{prayer}")
    return dict(key=f"{PLAN_VERSION}:{year}:{index + 1}", year=year, day=index + 1,
                week=week + 1, theme=theme, reference=reference,
                title=f"{theme} · {focus}", content=content,
                excerpt=reflection, passage=reference, source_title=ORIGINAL_SOURCE)


def _special_entry(year, leap=False):
    title = "An extra day of faithful service" if leap else "Remember, give thanks and begin again"
    reference = "Psalms 90:12" if leap else "Philippians 1:6"
    reflection = ("An extra date on the calendar is an ordinary opportunity to love God and serve a neighbour. "
                  "Receive the day with gratitude rather than pressure to achieve something impressive." if leap else
                  "Look back with honesty and gratitude. Growth includes quiet obedience, received correction and help "
                  "from other people. Entrust unfinished work to God and enter the next season ready to keep learning.")
    if year == 2 and not leap:
        reflection += " Consider who is now prepared to carry responsibility and whom you can help develop next."
    action = ("Offer practical help to someone whose work is often unnoticed." if leap else
              "Write down one lesson, thank someone who helped you grow and choose one habit to continue.")
    content = (f"{reflection}\n\nToday's reading: {reference}. Read it in context.\n\nReflect\n"
               "Where have you received grace, and how can that grace shape your next act of service?\n\n"
               f"Practise\n{action}\n\nPrayer\nLord, teach me to receive each day with gratitude and serve faithfully.")
    return dict(key=f"{PLAN_VERSION}:{year}:{'leap' if leap else 365}", year=year,
                day=366 if leap else 365, week=None, theme=title, reference=reference,
                title=title, content=content, excerpt=reflection, passage=reference,
                source_title=ORIGINAL_SOURCE)


# Materialised, reviewable daily entries; callers receive copies, not shared data.
TWO_YEAR_PLAN = tuple(_entry(year, index) for year in (1, 2) for index in range(364))
TWO_YEAR_PLAN = TWO_YEAR_PLAN[:364] + (_special_entry(1),) + TWO_YEAR_PLAN[364:] + (_special_entry(2),)
LEAP_DAY_ENTRY = _special_entry(1, leap=True)


def get_daily_entry(day):
    """Select by local calendar date; leap day does not shift later readings."""
    if not isinstance(day, date):
        raise TypeError("A local calendar date is required.")
    year = (day.year - ANCHOR_YEAR) % 2 + 1
    if (day.month, day.day) == (2, 29):
        entry = dict(LEAP_DAY_ENTRY)
        entry.update(year=year, key=f"{PLAN_VERSION}:{year}:leap")
        return entry
    index = (date(2025, day.month, day.day) - date(2025, 1, 1)).days
    return dict(TWO_YEAR_PLAN[(year - 1) * 365 + index])


def compose_devotional(entry, maxwell_note=None):
    """Keep our daily exercises distinct from the user's original book notes."""
    result = {key: entry[key] for key in ('title', 'content', 'excerpt', 'passage', 'source_title')}
    if maxwell_note:
        result.update(source_title=maxwell_note['source_title'],
                      content=(f"Maxwell Leadership Bible · {maxwell_note['title']}\n"
                               f"{maxwell_note.get('passage') or entry['reference']}\n\n{maxwell_note['content']}\n\n"
                               f"Original daily reflection\n{entry['content']}"))
    return result


def bible_study_passage(reference):
    """URL parameters understood by the Bible Study reader, without a lookup."""
    book, location = reference.rsplit(' ', 1)
    chapter, verse = location.split(':', 1)
    return dict(book="Psalms" if book == "Psalm" else book, chapter=int(chapter),
                verse=int(verse.split('-')[0]), translation='NIV')
