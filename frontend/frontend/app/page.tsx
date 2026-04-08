'use client';

import { useState, useEffect } from 'react';

const API_BASE = 'http://127.0.0.1:8000';

interface Company {
  company: string;
  avg_sentiment: number;
  total_mentions: number;
  trending_score: number;
}

interface ChatMessage {
  role: 'user' | 'bot';
  text: string;
}

export default function Home() {
  const [leaderboard, setLeaderboard] = useState<Company[]>([]);
  const [messages, setMessages] = useState<ChatMessage[]>([
    { role: 'bot', text: '👋 Hi! Ask me about any tech company sentiment. Try: "What is the sentiment on Anthropic?" or "Show me the leaderboard"' }
  ]);
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    fetch(`${API_BASE}/api/leaderboard`)
      .then(res => res.json())
      .then(data => setLeaderboard(data.leaderboard))
      .catch(err => console.error(err));
  }, []);

  const sendMessage = async () => {
    if (!input.trim()) return;
    const userMessage = input;
    setInput('');
    setMessages(prev => [...prev, { role: 'user', text: userMessage }]);
    setLoading(true);

    try {
      const res = await fetch(`${API_BASE}/api/chat`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ query: userMessage })
      });
      const data = await res.json();
      setMessages(prev => [...prev, { role: 'bot', text: data.response }]);
    } catch {
      setMessages(prev => [...prev, { role: 'bot', text: 'Error connecting to API.' }]);
    }
    setLoading(false);
  };

  const getSentimentColor = (score: number) => {
    if (score >= 0.05) return 'text-green-400';
    if (score <= -0.05) return 'text-red-400';
    return 'text-yellow-400';
  };

  const getSentimentEmoji = (score: number) => {
    if (score >= 0.05) return '🟢';
    if (score <= -0.05) return '🔴';
    return '🟡';
  };

  return (
    <main className="min-h-screen bg-gray-950 text-white p-6">
      <div className="max-w-6xl mx-auto">

        {/* Header */}
        <div className="mb-8">
          <h1 className="text-4xl font-bold text-white mb-2">
            ⚡ PulseBoard
          </h1>
          <p className="text-gray-400">
            Real-time brand & market intelligence — tracking 20 tech companies across HackerNews, GitHub & Reddit
          </p>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">

          {/* Leaderboard */}
          <div className="bg-gray-900 rounded-xl p-6 border border-gray-800">
            <h2 className="text-xl font-semibold mb-4 text-white">
              🏆 Trending Leaderboard
            </h2>
            {leaderboard.length === 0 ? (
              <p className="text-gray-500">Loading...</p>
            ) : (
              <div className="space-y-3">
                {leaderboard.map((company, index) => (
                  <div
                    key={company.company}
                    className="flex items-center justify-between p-3 bg-gray-800 rounded-lg"
                  >
                    <div className="flex items-center gap-3">
                      <span className="text-gray-500 text-sm w-6">{index + 1}</span>
                      <span>{getSentimentEmoji(company.avg_sentiment)}</span>
                      <span className="font-medium">{company.company}</span>
                    </div>
                    <div className="flex items-center gap-4 text-sm">
                      <span className={getSentimentColor(company.avg_sentiment)}>
                        {company.avg_sentiment >= 0 ? '+' : ''}{company.avg_sentiment.toFixed(3)}
                      </span>
                      <span className="text-gray-400">
                        {company.total_mentions} mentions
                      </span>
                      <span className="text-blue-400 font-semibold">
                        {company.trending_score.toFixed(1)}
                      </span>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>

          {/* Chat Interface */}
          <div className="bg-gray-900 rounded-xl p-6 border border-gray-800 flex flex-col h-[600px]">
            <h2 className="text-xl font-semibold mb-4 text-white">
              💬 Ask PulseBoard
            </h2>

            {/* Messages */}
            <div className="flex-1 overflow-y-auto space-y-3 mb-4">
              {messages.map((msg, index) => (
                <div
                  key={index}
                  className={`flex ${msg.role === 'user' ? 'justify-end' : 'justify-start'}`}
                >
                  <div
                    className={`max-w-xs lg:max-w-md px-4 py-2 rounded-xl text-sm whitespace-pre-wrap ${
                      msg.role === 'user'